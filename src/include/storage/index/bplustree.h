#pragma once

#include <cstring>
#include <functional>
#include <iostream>
#include <list>
#include <queue>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include "common/spin_latch.h"

namespace terrier::storage::index {

// This is the value we use in epoch manager to make sure
// no thread sneaking in while GC decision is being made
#define MAX_THREAD_COUNT ((int)0x7FFFFFFF)
// If node size goes above this then we split it
#define INNER_NODE_SIZE_UPPER_THRESHOLD ((int)128)
#define INNER_NODE_SIZE_LOWER_THRESHOLD ((int)32)

#define LEAF_NODE_SIZE_UPPER_THRESHOLD ((int)128)
#define LEAF_NODE_SIZE_LOWER_THRESHOLD ((int)32)
/*
 * class BPlusTreeBase - Base class of BPlusTree that stores some common members
 */
class BPlusTreeBase {
 public:
  // This is the presumed size of cache line
  static constexpr size_t CACHE_LINE_SIZE = 64;

  // This is the mask we used for address alignment (AND with this)
  static constexpr size_t CACHE_LINE_MASK = ~(CACHE_LINE_SIZE - 1);

  /** @return inner_node_size_upper_threshold */
  int GetInnerNodeSizeUpperThreshold() const { return inner_node_size_upper_threshold_; }
  /** @return inner_node_size_lower_threshold */
  int GetInnerNodeSizeLowerThreshold() const { return inner_node_size_lower_threshold_; }
  /** @return leaf_node_size_upper_threshold */
  int GetLeafNodeSizeUpperThreshold() const { return leaf_node_size_upper_threshold_; }
  /** @return leaf_node_size_lower_threshold */
  int GetLeafNodeSizeLowerThreshold() const { return leaf_node_size_lower_threshold_; }

  // Dont know whether we need to keep Padded Data - so removed that for now.
 public:
  /** @param inner_node_size_upper_threshold upper size threshold for inner node split to be assigned to this tree */
  void SetInnerNodeSizeUpperThreshold(int inner_node_size_upper_threshold) {
    inner_node_size_upper_threshold_ = inner_node_size_upper_threshold;
  }

  /** @param inner_node_size_upper_threshold lower size threshold for inner node removal to be assigned to this tree */
  void SetInnerNodeSizeLowerThreshold(int inner_node_size_lower_threshold) {
    inner_node_size_lower_threshold_ = inner_node_size_lower_threshold;
  }

  /** @param leaf_node_size_upper_threshold upper size threshold for leaf node split to be assigned to this tree */
  void SetLeafNodeSizeUpperThreshold(int leaf_node_size_upper_threshold) {
    leaf_node_size_upper_threshold_ = leaf_node_size_upper_threshold;
  }

  /** @param inner_node_size_upper_threshold lower size threshold for leaf node removal to be assigned to this tree */
  void SetLeafNodeSizeLowerThreshold(int leaf_node_size_lower_threshold) {
    leaf_node_size_lower_threshold_ = leaf_node_size_lower_threshold;
  }

 private:
 protected:
  /** upper size threshold for inner node split */
  int inner_node_size_upper_threshold_ = INNER_NODE_SIZE_UPPER_THRESHOLD;
  /** lower size threshold for inner node removal */
  int inner_node_size_lower_threshold_ = INNER_NODE_SIZE_LOWER_THRESHOLD;

  /** upper size threshold for leaf node split */
  int leaf_node_size_upper_threshold_ = LEAF_NODE_SIZE_UPPER_THRESHOLD;
  /** lower size threshold for leaf node removal */
  int leaf_node_size_lower_threshold_ = LEAF_NODE_SIZE_LOWER_THRESHOLD;

 public:
  /*
   * Constructor
   */
  BPlusTreeBase() = default;

  /*
   * Destructor
   */
  ~BPlusTreeBase() = default;
};

/*
 * class BPlusTree - Lock-free BPlusTree index implementation
 *
 * Template Arguments:
 *
 * template <typename KeyType,
 *           typename ValueType,
 *           typename KeyComparator = std::less<KeyType>,
 *           typename KeyEqualityChecker = std::equal_to<KeyType>,
 *           typename KeyHashFunc = std::hash<KeyType>,
 *           typename ValueEqualityChecker = std::equal_to<ValueType>,
 *           typename ValueHashFunc = std::hash<ValueType>>
 *
 * Explanation:
 *
 *  - KeyType: Key type of the map
 *
 *  - ValueType: Value type of the map. Note that it is possible
 *               that a single key is mapped to multiple values
 *
 *  - KeyComparator: "less than" relation comparator for KeyType
 *                   Returns true if "less than" relation holds
 *                   *** NOTE: THIS OBJECT DO NOT NEED TO HAVE A DEFAULT
 *                   CONSTRUCTOR.
 *                   Please refer to main.cpp, class KeyComparator for more
 *                   information on how to define a proper key comparator
 *
 *  - KeyEqualityChecker: Equality checker for KeyType
 *                        Returns true if two keys are equal
 *
 *  - KeyHashFunc: Hashes KeyType into size_t. This is used in unordered_set
 *
 *  - ValueEqualityChecker: Equality checker for value type
 *                          Returns true for ValueTypes that are equal
 *
 *  - ValueHashFunc: Hashes ValueType into a size_t
 *                   This is used in unordered_set
 *
 * If not specified, then by default all arguments except the first two will
 * be set as the standard operator in C++ (i.e. the operator for primitive types
 * AND/OR overloaded operators for derived types)
 */
template <typename KeyType, typename ValueType, typename KeyComparator = std::less<KeyType>,
          typename KeyEqualityChecker = std::equal_to<KeyType>, typename KeyHashFunc = std::hash<KeyType>,
          typename ValueEqualityChecker = std::equal_to<ValueType>, typename ValueHashFunc = std::hash<ValueType>>
class BPlusTree : public BPlusTreeBase {
 public:
  class BaseNode;
  class KeyNodePointerPairComparator;
  class KeyNodePointerPairHashFunc;
  class KeyNodePointerPairEqualityChecker;
  class KeyValuePairHashFunc;
  class KeyValuePairEqualityChecker;

  // KeyType-NodeID pair
  using KeyNodePointerPair = std::pair<KeyType, BaseNode *>;
  // KeyType - List of ValueType pair
  using KeyValuePair = std::pair<KeyType, std::list<ValueType> *>;

  using KeyElementPair = std::pair<KeyType, ValueType>;

  /*
   * enum class NodeType - Bw-Tree node type
   */
  enum class NodeType : short {
    InnerType = 0,
    // Data page type
    LeafType = 1
  };

  ///////////////////////////////////////////////////////////////////
  // Comparator, equality checker and hasher for key-NodeID pair
  ///////////////////////////////////////////////////////////////////

  /*
   * class KeyNodePointerPairComparator - Compares key-value pair for < relation
   *
   * Only key values are compares. However, we should use WrappedKeyComparator
   * instead of the raw one, since there could be -Inf involved in inner nodes
   */
  class KeyNodePointerPairComparator {
   public:
    const KeyComparator *key_cmp_obj_p_;

    /*
     * Default constructor - deleted
     */
    KeyNodePointerPairComparator() = delete;

    /*
     * Constructor - Initialize a key-NodeID pair comparator using
     *               wrapped key comparator
     */
    explicit KeyNodePointerPairComparator(BPlusTree *p_tree_p) : key_cmp_obj_p_{&p_tree_p->key_cmp_obj_} {}

    /*
     * operator() - Compares whether a key NodeID pair is less than another
     *
     * We only compare keys since there should not be duplicated
     * keys inside an inner node
     */
    bool operator()(const KeyNodePointerPair &knp1, const KeyNodePointerPair &knp2) const {
      // First compare keys for relation
      return (*key_cmp_obj_p_)(knp1.first, knp2.first);
    }
  };

  /*
   * class KeyNodePointerPairEqualityChecker - Checks KeyNodePointerPair equality
   *
   * Only keys are checked since there should not be duplicated keys inside
   * inner nodes. However we should always use wrapped key eq checker rather
   * than wrapped raw key eq checker
   */
  class KeyNodePointerPairEqualityChecker {
   public:
    const KeyEqualityChecker *key_eq_obj_p_;

    /*
     * Default constructor - deleted
     */
    KeyNodePointerPairEqualityChecker() = delete;

    /*
     * Constructor - Initialize a key node pair eq checker
     */
    explicit KeyNodePointerPairEqualityChecker(BPlusTree *p_tree_p) : key_eq_obj_p_{&p_tree_p->key_eq_obj_} {}

    /*
     * operator() - Compares key-NodeID pair by comparing keys
     */
    bool operator()(const KeyNodePointerPair &knp1, const KeyNodePointerPair &knp2) const {
      return (*key_eq_obj_p_)(knp1.first, knp2.first);
    }
  };

  /*
   * class KeyNodePointerPairHashFunc - Hashes a key-NodeID pair into size_t
   */
  class KeyNodePointerPairHashFunc {
   public:
    const KeyHashFunc *key_hash_obj_p_;

    /*
     * Default constructor - deleted
     */
    KeyNodePointerPairHashFunc() = delete;

    /*
     * Constructor - Initialize a key value pair hash function
     */
    explicit KeyNodePointerPairHashFunc(BPlusTree *p_tree_p) : key_hash_obj_p_{&p_tree_p->key_hash_obj_} {}

    /*
     * operator() - Hashes a key-value pair by hashing each part and
     *              combine them into one size_t
     *
     * We use XOR to combine hashes of the key and value together into one
     * single hash value
     */
    size_t operator()(const KeyNodePointerPair &knp) const { return (*key_hash_obj_p_)(knp.first); }
  };

  ///////////////////////////////////////////////////////////////////
  // Comparator, equality checker and hasher for key-value pair
  ///////////////////////////////////////////////////////////////////

  /*
   * class KeyValuePairComparator - Comparator class for KeyValuePair
   */
  class KeyValuePairComparator {
   public:
    const KeyComparator *key_cmp_obj_p_;

    /*
     * Default constructor - deleted
     */
    KeyValuePairComparator() = delete;

    /*
     * Constructor
     */
    explicit KeyValuePairComparator(BPlusTree *p_tree_p) : key_cmp_obj_p_{&p_tree_p->key_cmp_obj_} {}

    /*
     * operator() - Compares key-value pair by comparing each component
     *              of them
     *
     * NOTE: This function only compares keys with KeyType. For +/-Inf
     * the wrapped raw key comparator will fail
     */
    bool operator()(const KeyValuePair &kvp1, const KeyValuePair &kvp2) const {
      return (*key_cmp_obj_p_)(kvp1.first, kvp2.first);
    }
  };

  /*
   * class KeyValuePairEqualityChecker - Checks KeyValuePair equality
   */
  class KeyValuePairEqualityChecker {
   public:
    const KeyEqualityChecker *key_eq_obj_p_;
    const ValueEqualityChecker *value_eq_obj_p_;

    /*
     * Default constructor - deleted
     */
    KeyValuePairEqualityChecker() = delete;

    /*
     * Constructor - Initialize a key value pair equality checker with
     *               WrappedKeyEqualityChecker and ValueEqualityChecker
     */
    explicit KeyValuePairEqualityChecker(BPlusTree *p_tree_p)
        : key_eq_obj_p_{&p_tree_p->key_eq_obj_}, value_eq_obj_p_{&p_tree_p->value_eq_obj_} {}

    /*
     * operator() - Compares key-value pair by comparing each component
     *              of them
     *
     * NOTE: This function only compares keys with KeyType. For +/-Inf
     * the wrapped raw key comparator will fail
     */
    bool operator()(const KeyValuePair &kvp1, const KeyValuePair &kvp2) const {
      return ((*key_eq_obj_p_)(kvp1.first, kvp2.first)) && ((*value_eq_obj_p_)(kvp1.second, kvp2.second));
    }
  };

  ///////////////////////////////////////////////////////////////////
  // Key Comparison Member Functions
  ///////////////////////////////////////////////////////////////////

  /*
   * KeyCmpLess() - Compare two keys for "less than" relation
   *
   * If key1 < key2 return true
   * If not return false
   *
   * NOTE: In older version of the implementation this might be defined
   * as the comparator to wrapped key type. However wrapped key has
   * been removed from the newest implementation, and this function
   * compares KeyType specified in template argument.
   */
  bool KeyCmpLess(const KeyType &key1, const KeyType &key2) const { return key_cmp_obj_(key1, key2); }

  /*
   * KeyCmpEqual() - Compare a pair of keys for equality
   *
   * This functions compares keys for equality relation
   */
  bool KeyCmpEqual(const KeyType &key1, const KeyType &key2) const { return key_eq_obj_(key1, key2); }

  /*
   * KeyCmpGreaterEqual() - Compare a pair of keys for >= relation
   *
   * It negates result of keyCmpLess()
   */
  bool KeyCmpGreaterEqual(const KeyType &key1, const KeyType &key2) const { return !KeyCmpLess(key1, key2); }

  /*
   * KeyCmpGreater() - Compare a pair of keys for > relation
   *
   * It flips input for keyCmpLess()
   */
  bool KeyCmpGreater(const KeyType &key1, const KeyType &key2) const { return KeyCmpLess(key2, key1); }

  /*
   * KeyCmpLessEqual() - Compare a pair of keys for <= relation
   */
  bool KeyCmpLessEqual(const KeyType &key1, const KeyType &key2) const { return !KeyCmpGreater(key1, key2); }

  ///////////////////////////////////////////////////////////////////
  // Value Comparison Member
  ///////////////////////////////////////////////////////////////////

  /*
   * ValueCmpEqual() - Compares whether two values are equal
   */
  bool ValueCmpEqual(const ValueType &v1, const ValueType &v2) { return value_eq_obj_(v1, v2); }

  /*
   * class NodeMetaData - Holds node metadata in an object
   *
   * Node metadata includes a pointer to the range object, the depth
   * of the current delta chain (NOTE: If there is a merge node then the
   * depth is the sum of the length of its two children rather than
   * the larger one)
   *
   * Since we need to query for high key and low key in every step of
   * traversal down the tree (i.e. on each level we need to verify we
   * are on the correct node). It would be wasteful if we traverse down the
   * delta chain down to the bottom everytime to collect these metadata
   * therefore as an optimization we store them inside each delta node
   * and leaf/inner node to optimize for performance
   *
   * NOTE: We do not count node type as node metadata
   */
  class NodeMetaData {
   public:
    // For all nodes including base node and data node and SMO nodes,
    // the low key pointer always points to a KeyNodePointerPair structure
    // inside the base node, which is either the first element of the
    // node sep list (InnerNode), or a class member (LeafNode)
    const KeyNodePointerPair *low_key_p_;

    // high key points to the KeyNodePointerPair inside the LeafNode and InnerNode
    // if there is neither SplitNode nor MergeNode. Otherwise it
    // points to the item inside split node or merge right sibling branch
    const KeyNodePointerPair *high_key_p_;

    // The type of the node; this is forced to be represented as a short type
    NodeType type_;

    // This is the height of the node
    int depth_;

    // This counts the total number of items in the node
    int item_count_;

    /*
     * Constructor
     */
    NodeMetaData(const KeyNodePointerPair *p_low_key_p, const KeyNodePointerPair *p_high_key_p, NodeType p_type,
                 int p_depth, int p_item_count)
        : low_key_p_{p_low_key_p},
          high_key_p_{p_high_key_p},
          type_{p_type},
          depth_{p_depth},
          item_count_{p_item_count} {}
  };

  /*
   * class BaseNode - Generic node class; inherited by leaf, inner
   *                  and delta node
   */
  class BaseNode {
    // We hold its data structure as private to force using member functions
    // for member access
   private:
    // This holds low key, high key, next node ID, type, depth and item count
    NodeMetaData metadata_;

   public:
    /*
     * Constructor - Initialize type and metadata
     */
    BaseNode(NodeType p_type, const KeyNodePointerPair *p_low_key_p, const KeyNodePointerPair *p_high_key_p,
             int p_depth, int p_item_count)
        : metadata_{p_low_key_p, p_high_key_p, p_type, p_depth, p_item_count} {}

    /*
     * Type() - Return the type of node
     *
     * This method does not allow overridding
     */
    NodeType GetType() const { return metadata_.type_; }

    /*
     * GetNodeMetaData() - Returns a const reference to node metadata
     *
     * Please do not override this method
     */
    const NodeMetaData &GetNodeMetaData() const { return metadata_; }

    /*
     * IsInnerNode() - Returns true if the node is an inner node
     *
     * This is useful if we want to collect all seps on an inner node
     * If the top of delta chain is an inner node then just do not collect
     * and use the node directly
     */
    bool IsInnerNode() const { return GetType() == NodeType::InnerType; }

    /*
     * GetLowKey() - Returns the low key of the current base node
     *
     * NOTE: Since it is defined that for LeafNode the low key is undefined
     * and pointers should be set to nullptr, accessing the low key of
     * a leaf node would result in Segmentation Fault
     */
    const KeyType &GetLowKey() const { return metadata_.low_key_p->first; }

    /*
     * GetHighKey() - Returns a reference to the high key of current node
     *
     * This function could be called for all node types including leaf nodes
     * and inner nodes.
     */
    const KeyType &GetHighKey() const { return metadata_.high_key_p_->first; }

    /*
     * GetHighKeyPair() - Returns the pointer to high key node id pair
     */
    const KeyNodePointerPair &GetHighKeyPair() const { return *metadata_.high_key_p_; }

    /*
     * GetLowKeyPair() - Returns the pointer to low key node id pair
     *
     * The return value is nullptr for LeafNode and its delta chain
     */
    const KeyNodePointerPair &GetLowKeyPair() const { return *metadata_.low_key_p_; }

    /*
     * GetNextNodeID() - Returns the next NodeID of the current node
     */
    BaseNode *GetNextNodeID() const { return metadata_.high_key_p_->second; }

    /*
     * GetLowKeyNodeID() - Returns the NodeID for low key
     *
     * NOTE: This function should not be called for leaf nodes
     * since the low key node ID for leaf node is not defined
     */
    BaseNode *GetLowKeyNodeID() const { return metadata_.low_key_p_->second; }

    /*
     * GetDepth() - Returns the Height of the Node
     */
    int GetDepth() const { return metadata_.depth_; }

    /*
     * GetItemCount() - Returns the item count of the current node
     */
    int GetItemCount() const { return metadata_.item_count_; }

    /*
     * SetLowKeyPair() - Sets the low key pair of metadata
     */
    void SetLowKeyPair(const KeyNodePointerPair *p_low_key_p) { metadata_.low_key_p_ = p_low_key_p; }

    /*
     * SetHighKeyPair() - Sets the high key pair of metdtata
     */
    void SetHighKeyPair(const KeyNodePointerPair *p_high_key_p) { metadata_.high_key_p_ = p_high_key_p; }

    /*
      SetType() - Sets the type of metadata
    */
    void SetType(const NodeType input_type) { metadata_.type_ = input_type; }

    /*
      SetDepth() - Sets the depth of metadata
    */
    void SetDepth(const int input_depth) { metadata_.depth_ = input_depth; }

    /*
      SetItemCount() - Sets the item_count of metadata
    */
    void SetItemCount(const int input_item_count) { metadata_.item_count_ = input_item_count; }
  };

  /*
   * class ElasticNode - The base class for elastic node types, i.e. InnerNode
   *                     and LeafNode
   *
   * Since for InnerNode and LeafNode, the number of elements is not a compile
   * time known constant. However, for efficient tree traversal we must
   * all elements to reduce cache misses with workload that's less predictable
   */
  template <typename ElementType>
  class ElasticNode : public BaseNode {
   private:
    // These two are the low key and high key of the node respectively
    // since we could not add it in the inherited class (will clash with
    // the array which is invisible to the compiler) so they must be added here
    KeyNodePointerPair low_key_;
    KeyNodePointerPair high_key_;

    // This is the end of the elastic array
    // We explicitly store it here to avoid calculating the end of the array
    // everytime
    ElementType *end_;

    // This is the starting point
    ElementType start_[0];

   public:
    /*
     * Constructor
     *
     * Note that this constructor uses the low key and high key stored as
     * members to initialize the NodeMetadata object in class BaseNode
     */
    ElasticNode(NodeType p_type, int p_depth, int p_item_count, const KeyNodePointerPair &p_low_key,
                const KeyNodePointerPair &p_high_key)
        : BaseNode{p_type, &low_key_, &high_key_, p_depth, p_item_count},
          low_key_{p_low_key},
          high_key_{p_high_key},
          end_{start_} {}

    /*
     * Copy() - Copy constructs another instance
     */
    static ElasticNode *Copy(const ElasticNode &other) {
      ElasticNode *node_p = ElasticNode::Get(other.GetItemCount(), other.GetType(), other.GetDepth(),
                                             other.GetItemCount(), other.GetLowKeyPair(), other.GetHighKeyPair());

      node_p->PushBack(other.Begin(), other.End());

      return node_p;
    }

    void FreeElasticNode() {
      for (ElementType *element_p = Begin(); element_p != End(); element_p++) {
        // Manually calls destructor when the node is destroyed
        element_p->~ElementType();
      }
      ElasticNode *beginning_allocation = this;
      delete[] reinterpret_cast<char *>(beginning_allocation);
    }

    /*
     IntializeEnd() - Make end = start
    */
    void InitializeEnd() { end_ = start_; }

    /*
     SetEnd() - Make end = start + offset
    */
    void SetEnd(int offset) { end_ = start_ + offset; }

    /*
     * Begin() - Returns a begin iterator to its internal array
     */
    ElementType *Begin() { return start_; }

    const ElementType *Begin() const { return start_; }

    /*
     * End() - Returns an end iterator that is similar to the one for vector
     */
    ElementType *End() { return end_; }

    const ElementType *End() const { return end_; }

    /*
     * REnd() - Returns the element before the first element
     *
     * Note that since we returned an invalid pointer into the array, the
     * return value should not be modified and is therefore of const type
     */
    const ElementType *REnd() { return start_ - 1; }

    const ElementType *REnd() const { return start_ - 1; }

    /*
     * PushBack() - Push back an element
     *
     * This function takes an element type and copy-construct it on the array
     * which is invisible to the compiler. Therefore we must call placement
     * operator new to do the job
     */
    void PushBack(const ElementType &element) {
      // Placement new + copy constructor using end pointer
      new (end_) ElementType{element};

      // Move it pointing to the enxt available slot, if not reached the end
      end_++;
    }

    /*
     * PushBack() - Push back a series of elements
     *
     * The overloaded PushBack() could also push an array of elements
     */
    void PushBack(const ElementType *copy_start_p, const ElementType *copy_end_p) {
      // Make sure the loop will come to an end
      TERRIER_ASSERT(copy_start_p <= copy_end_p, "Loop will not come to an end.");

      while (copy_start_p != copy_end_p) {
        PushBack(*copy_start_p);
        copy_start_p++;
      }
    }

   public:
    /*
     Print elastic node keys
   */
    void PrintElasticNode() {
      for (ElementType *element_p = Begin(); element_p != End(); element_p++) {
        // Manually calls destructor when the node is destroyed
        // std::cout << element_p->first << " ";
      }
    }
    /*
    insertElementIfPossible - Returns true if inserted and false if node full
    Inserts at location provided.
    */
    bool InsertElementIfPossible(const ElementType &element, ElementType *location) {
      if (GetSize() >= this->GetItemCount()) return false;
      if (end_ - location > 0)
        std::memmove(reinterpret_cast<void *>(location + 1), reinterpret_cast<void *>(location),
                     (end_ - location) * sizeof(ElementType));
      new (location) ElementType{element};
      end_ = end_ + 1;
      return true;
    }

    /*
    SplitNode - Splits the current node and returns a pointer to the new node.
    Returns NULL if node is not already full
    */
    ElasticNode *SplitNode() {
      if (this->GetSize() < this->GetItemCount()) return nullptr;
      ElasticNode *new_node = this->Get(this->GetSize(), this->GetType(), this->GetDepth(), this->GetItemCount(),
                                        *this->GetElasticLowKeyPair(), *this->GetElasticHighKeyPair());
      ElementType *copy_from_location = Begin() + ((this->GetSize()) / 2);
      // Can be memcopy
      std::memmove(reinterpret_cast<void *>(new_node->Begin()), reinterpret_cast<void *>(copy_from_location),
                   (end_ - copy_from_location) * sizeof(ElementType));
      new_node->SetEnd((end_ - copy_from_location));
      end_ = copy_from_location;
      return new_node;
    }

    /*
    Pop Begin - Pops the first element from the list
    Returns False if empty
    */
    bool PopBegin() {
      if (this->GetSize() == 0) return false;
      if (this->GetSize() == 1) {
        SetEnd(0);
        return true;
      }
      std::memmove(reinterpret_cast<void *>(start_), reinterpret_cast<void *>(start_ + 1),
                   (this->GetSize() - 1) * sizeof(ElementType));
      SetEnd(this->GetSize() - 1);
      return true;
    }

    /*
    FindLocation - Returns the start of the first element that compares
    greater to the key provided
    */
    ElementType *FindLocation(const KeyType &element, BPlusTree *tree) {
      ElementType *iter = Begin();
      while ((iter != end_) && (!tree->KeyCmpGreater(iter->first, element))) iter++;
      return iter;
    }

    /*
     * GetSize() - Returns the size of the embedded list
     *
     * Note that the return type is integer since we use integer to represent
     * the size of a node
     */
    int GetSize() const { return static_cast<int>(End() - Begin()); }
    /*
     * SetElasticLowKeyPair() - Sets the low key of Elastic Node
     */
    void SetElasticLowKeyPair(const KeyNodePointerPair &p_low_key) { low_key_ = p_low_key; }

    /*
     * SetElasticHighKeyPair() - Sets the high key of Elastic Node
     */
    void SetElasticHighKeyPair(const KeyNodePointerPair &p_high_key) { high_key_ = p_high_key; }

    /*
     * GetElasticLowKeyPair() - Returns a pointer to the low key current Elastic node
     */
    KeyNodePointerPair *GetElasticLowKeyPair() { return &low_key_; }

    /*
     * GetElasticHighKeyPair() - Returns a pointer to the high key of current Elastic node
     */
    KeyNodePointerPair *GetElasticHighKeyPair() { return &high_key_; }

    /*
     * Get() - Static helper function that constructs a elastic node of
     *         a certain size
     *
     * Note that since operator new is only capable of allocating a fixed
     * sized structure, we need to call malloc() directly to deal with variable
     * lengthed node. However, after malloc() returns we use placement operator
     * new to initialize it, such that the node could be freed using operator
     * delete later on
     */
    static ElasticNode *Get(int size,  // Number of elements
                            NodeType p_type, int p_depth,
                            int p_item_count,  // Usually equal to size
                            const KeyNodePointerPair &p_low_key, const KeyNodePointerPair &p_high_key) {
      // Allocte memory for
      //   1. AllocationMeta (chunk)
      //   2. node meta
      //   3. ElementType array
      // basic template + ElementType element size * (node size) + CHUNK_SIZE()
      // Note: do not make it constant since it is going to be modified
      // after being returned
      auto *alloc_base = new char[sizeof(ElasticNode) + size * sizeof(ElementType)];
      auto elastic_node = reinterpret_cast<ElasticNode *>(alloc_base);
      elastic_node->SetLowKeyPair(elastic_node->GetElasticLowKeyPair());
      elastic_node->SetHighKeyPair(elastic_node->GetElasticHighKeyPair());
      elastic_node->SetItemCount(p_item_count);
      elastic_node->SetType(p_type);
      elastic_node->SetDepth(p_depth);
      elastic_node->SetElasticLowKeyPair(p_low_key);
      elastic_node->SetElasticHighKeyPair(p_high_key);
      elastic_node->InitializeEnd();
      return elastic_node;
    }

    /*
     * GetNodeHeader() - Given low key pointer, returns the node header
     *
     * This is useful since only the low key pointer is available from any
     * type of node
     */
    static ElasticNode *GetNodeHeader(const KeyNodePointerPair *low_key_p) {
      static constexpr size_t low_key_offset = offsetof(ElasticNode, low_key);

      return reinterpret_cast<ElasticNode *>(reinterpret_cast<uint64_t>(low_key_p) - low_key_offset);
    }

    /*
     * At() - Access element with bounds checking under debug mode
     */
    ElementType &At(const int index) {
      // The index must be inside the valid range
      TERRIER_ASSERT(index < GetSize(), "Index out of range.");

      return *(Begin() + index);
    }

    const ElementType &At(const int index) const {
      // The index must be inside the valid range
      TERRIER_ASSERT(index < GetSize(), "Index out of range.");

      return *(Begin() + index);
    }
  };

  /*
   * class InnerNode - Inner node that holds separators
   */
  class InnerNode : public ElasticNode<KeyNodePointerPair> {
   public:
    /*
     * Constructor - Deleted
     *
     * All construction of InnerNode should be through ElasticNode interface
     */
    InnerNode() = delete;
    InnerNode(const InnerNode &) = delete;
    InnerNode(InnerNode &&) = delete;
    InnerNode &operator=(const InnerNode &) = delete;
    InnerNode &operator=(InnerNode &&) = delete;

    /*
     * Destructor - Calls destructor of ElasticNode
     */
    ~InnerNode() { this->~ElasticNode<KeyNodePointerPair>(); }
  };

  /*
   * class LeafNode - Leaf node that holds data
   *
   * There are 5 types of delta nodes that could be appended
   * to a leaf node. 3 of them are SMOs, and 2 of them are data operation
   */
  class LeafNode : public ElasticNode<KeyValuePair> {
   public:
    LeafNode() = delete;
    LeafNode(const LeafNode &) = delete;
    LeafNode(LeafNode &&) = delete;
    LeafNode &operator=(const LeafNode &) = delete;
    LeafNode &operator=(LeafNode &&) = delete;

    /*
     * Destructor - Calls underlying ElasticNode d'tor
     */
    ~LeafNode() { this->~ElasticNode<KeyValuePair>(); }
  };

  // Key comparator
  const KeyComparator key_cmp_obj_;

  // Raw key eq checker
  const KeyEqualityChecker key_eq_obj_;

  // Raw key hasher
  const KeyHashFunc key_hash_obj_;

  // Check whether values are equivalent
  const ValueEqualityChecker value_eq_obj_;

  // Hash ValueType into a size_t
  const ValueHashFunc value_hash_obj_;

  // The following three are used for std::pair<KeyType, NodeID>
  const KeyNodePointerPairComparator key_node_id_pair_cmp_obj_;
  const KeyNodePointerPairEqualityChecker key_node_id_pair_eq_obj_;

  // The following two are used for hashing KeyValuePair
  const KeyValuePairComparator key_value_pair_cmp_obj_;
  const KeyValuePairEqualityChecker key_value_pair_eq_obj_;

 private:
  BaseNode *root_;
  mutable common::SpinLatch root_latch_;

 public:
  /*
    Get Root - Returns the current root
  */
  BaseNode *GetRoot() { return root_; }

  /*
    Get Element
  */
  KeyElementPair GetElement(KeyType key, ValueType value) {
    KeyElementPair p1;
    p1.first = key;
    p1.second = value;
    return p1;
  }

  /*
    Tries to find key by Traversing down the BplusTree
    Returns true if found
    Returns false if not found
  */
  bool IsPresent(KeyType key) {
    if (root_ == nullptr) {
      return false;
    }

    BaseNode *current_node = root_;

    // Traversing Down to the right leaf node
    while (current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = node->FindLocation(key, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      if (index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      } else
        current_node = node->GetLowKeyPair().second;
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    for (KeyValuePair *element_p = node->Begin(); element_p != node->End(); element_p++) {
      if (element_p->first == key) {
        return true;
      }
    }

    return false;
  }

  /*
    Tries to find key by Traversing down the BplusTree
    Returns the list of values of the key from leaf if found
    Returns null if not found
  */
  void FindValueOfKey(KeyType key, std::vector<ValueType> &result) {
    common::SpinLatch::ScopedSpinLatch guard(&root_latch_);

    if (root_ == nullptr) {
      return;
    }

    BaseNode *current_node = root_;

    // Traversing Down to the right leaf node
    while (current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = node->FindLocation(key, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      if (index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      } else
        current_node = node->GetLowKeyPair().second;
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    for (KeyValuePair *element_p = node->Begin(); element_p != node->End(); element_p++) {
      if (KeyCmpEqual(element_p->first, key)) {
        auto itr_list = element_p->second->begin();
        while (itr_list != element_p->second->end()) {
          result.push_back(*itr_list);
          itr_list++;
        }
        return;
      }
    }
  }

  void PrintTreeNode(BaseNode *node) {
    if (node->GetType() != NodeType::LeafType) {
      auto node_elastic = reinterpret_cast<ElasticNode<KeyValuePair> *>(node);
      node_elastic->PrintElasticNode();
    } else {
      auto node_elastic = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);
      node_elastic->PrintElasticNode();
    }
    // std::cout << '\t';
  }

  /*
    Traverses Down the root in a BFS manner and prints all the nodes
  */
  void PrintTree() {
    if (root_ == NULL) return;
    std::queue<BaseNode *> bfs_queue;
    std::queue<BaseNode *> all_nodes;
    bfs_queue.push(root_);

    // print root
    // std::cout << "Printing Root " << std::endl;
    if (root_->GetType() != NodeType::LeafType) {
      auto root_elastic = reinterpret_cast<ElasticNode<KeyValuePair> *>(root_);
      root_elastic->PrintElasticNode();
    } else {
      auto root_elastic = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(root_);
      root_elastic->PrintElasticNode();
    }

    while (!bfs_queue.empty()) {
      BaseNode *node = bfs_queue.front();
      bfs_queue.pop();
      all_nodes.push(node);
      if (node->GetType() != NodeType::LeafType) {
        // std::cout << "printing children " << std::endl;

        bfs_queue.push(node->GetLowKeyPair().second);
        PrintTreeNode(node->GetLowKeyPair().second);

        auto current_node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);
        for (KeyNodePointerPair *element_p = current_node->Begin(); element_p != current_node->End(); element_p++) {
          bfs_queue.push(element_p->second);
          PrintTreeNode(element_p->second);
        }
      }
    }
    // std::cout << std::endl;
  }

  /*
    Traverses Down the root in a BFS manner and frees all the nodes
  */
  void FreeTree() {
    if (root_ == nullptr) return;
    std::queue<BaseNode *> bfs_queue;
    std::queue<BaseNode *> all_nodes;
    bfs_queue.push(root_);

    while (!bfs_queue.empty()) {
      BaseNode *node = bfs_queue.front();
      bfs_queue.pop();
      all_nodes.push(node);
      if (node->GetType() != NodeType::LeafType) {
        bfs_queue.push(node->GetLowKeyPair().second);
        auto current_node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);
        for (KeyNodePointerPair *element_p = current_node->Begin(); element_p != current_node->End(); element_p++) {
          bfs_queue.push(element_p->second);
        }
      }
    }

    while (!all_nodes.empty()) {
      BaseNode *node = all_nodes.front();
      all_nodes.pop();
      if (node->GetType() != NodeType::LeafType) {
        auto current_node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);
        current_node->FreeElasticNode();
      } else {
        auto current_node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node);
        for (KeyValuePair *element_p = current_node->Begin(); element_p != current_node->End(); element_p++) {
          // destroy the list of values in a key
          delete element_p->second;
        }
        current_node->FreeElasticNode();
      }
    }
  }

  /*
  StructuralIntegrityVerification - Recursively Tests the structural integrity of the data
  structure. Verifies whether all the keys in the set are present in the tree. Verifies
  all keys lie between the given range(This gets pruned as move down the tree). Also verifies
  the size of each node.
  Also erases all keys that are found in the tree from the input set
  */
  bool StructuralIntegrityVerification(KeyType low_key, KeyType high_key, std::set<KeyType> &keys,
                                       BaseNode *current_node) {
    bool return_answer = true;
    if (current_node->GetType() == NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
      for (KeyValuePair *element_p = node->Begin(); element_p != node->End(); element_p++) {
        /* All Keys within Range*/
        if (this->KeyCmpGreater(low_key, element_p->first) || this->KeyCmpGreater(element_p->first, high_key)) {
          return false;
        }
        /* Size of Node is correct */
        if (current_node != root_)
          if (node->GetSize() < leaf_node_size_lower_threshold_ || node->GetSize() > leaf_node_size_upper_threshold_) {
            return false;
          }
        keys.erase(element_p->first);
      }
    } else {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      /* Size of Node is correct */
      if (current_node != root_)
        if (node->GetSize() < inner_node_size_lower_threshold_ || node->GetSize() > inner_node_size_upper_threshold_) {
          return false;
        }
      return_answer &= this->StructuralIntegrityVerification(low_key, node->Begin()->first, keys,
                                                             current_node->GetLowKeyPair().second);

      KeyNodePointerPair *iter = node->Begin();
      while ((iter + 1) != node->End()) {
        low_key = iter->first;
        return_answer &= this->StructuralIntegrityVerification(low_key, (iter + 1)->first, keys, iter->second);
        iter++;
      }

      return_answer &= this->StructuralIntegrityVerification(iter->first, high_key, keys, iter->second);
    }
    return return_answer;
  }

  /*
    SiblingForwardCheck - Expects a sorted set of the keys and then finds the
    the least element and the leaf it is stored in. Traverses the right sibling
    of the leaf to cover all the elements in the b+ tree and checks with the
    sorted key set for their order. Verifies if a full scan (ascending) using the siblings
    is correct or not.
  */
  bool SiblingForwardCheck(std::set<KeyType> &keys) {
    int key = *keys.begin();
    if (root_ == nullptr) {
      return false;
    }

    BaseNode *current_node = root_;

    // Traversing Down to the right leaf node
    while (current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = node->FindLocation(key, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      if (index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      } else
        current_node = node->GetLowKeyPair().second;
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    auto itr = keys.begin();
    while (node != nullptr) {
      // iterate over all the values in the leaf node checking with the key set
      for (KeyValuePair *element_p = node->Begin(); element_p != node->End(); element_p++) {
        if (element_p->first != *itr) {
          return false;
        }
        itr++;
      }
      // current node checked, move to the next one
      node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node->GetHighKeyPair().second);
    }
    return true;
  }

  /*
    SiblingBackwardCheck - Expects a sorted set of the keys and then finds the
    the highest element and the leaf it is stored in. Traverses the left sibling
    of the leaf to cover all the elements in the b+ tree and checks with the
    sorted key set for their order. Verifies if a full scan (descending) using the siblings
    is correct or not.
  */
  bool SiblingBackwardCheck(std::set<KeyType> &keys) {
    int key = *keys.rbegin();
    if (root_ == nullptr) {
      return false;
    }

    BaseNode *current_node = root_;

    // Traversing Down to the right leaf node
    while (current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = node->FindLocation(key, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      if (index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      } else
        current_node = node->GetLowKeyPair().second;
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    auto itr = keys.rbegin();
    while (node != nullptr) {
      // iterate over all the values in the leaf node checking with the key set
      for (KeyValuePair *element_p = node->End() - 1; element_p != node->REnd(); element_p--) {
        if (element_p->first != *itr) {
          return false;
        }
        itr++;
      }
      // current node checked, move to the next one
      node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node->GetLowKeyPair().second);
    }
    return true;
  }

  /*
   * This test checks if duplicate keys are correctly handled by the insert wrapper.
   * For a b+ tree, with each key having multiple values, stored in a map keys_values
   * check if the values_list in the tree have the same values as the map given
   * in the function.
   */
  bool DuplicateKeyValuesCheck(std::unordered_map<KeyType, std::vector<ValueType>> &keys_values) {
    auto itr = keys_values.begin();
    for (; itr != keys_values.end(); itr++) {
      KeyType k = itr->first;
      std::vector<ValueType> values = keys_values[k];
      std::vector<ValueType> result;
      FindValueOfKey(k, result);
      if (result.empty()) {
        return false;
      }
      auto it = result.begin();
      for (unsigned j = 0; j < values.size(); j++) {
        if (it == result.end()) {
          return false;
        }
        if (values[j] != *(it)) {
          return false;
        }
        it++;
      }
    }
    return true;
  }

  /*
   * The check gets a map of keys to vector of values that are whetted and
   * removes any duplicates that might have been tried to be added by a
   * transaction in insert unique. The check ensures that the values list
   * in the B+ tree also have the same keys and the same order.
   */
  bool DuplicateKeyValueUniqueInsertCheck(std::unordered_map<KeyType, std::vector<ValueType>> &keys_values) {
    auto itr = keys_values.begin();
    for (; itr != keys_values.end(); itr++) {
      KeyType k = itr->first;
      std::vector<ValueType> values = keys_values[k];
      std::list<ValueType> *values_list_p = FindValueOfKey(k);
      if (values_list_p == NULL) {
        return false;
      }
      auto it_list = values_list_p->begin();
      for (unsigned j = 0; j != values.size(); j++) {
        if (it_list == values_list_p->end()) {
          return false;
        }
        if (values[j] != *(it_list)) {
          return false;
        }
        it_list++;
      }
    }
    return true;
  }

  /*
  Returns current heap usage
  */
  size_t GetHeapUsage() {
    common::SpinLatch::ScopedSpinLatch guard(&root_latch_);
    if (root_ == nullptr) return 0;

    std::queue<BaseNode *> bfs_queue;
    std::queue<BaseNode *> all_nodes;
    bfs_queue.push(root_);

    while (!bfs_queue.empty()) {
      BaseNode *node = bfs_queue.front();
      bfs_queue.pop();
      all_nodes.push(node);
      if (node->GetType() != NodeType::LeafType) {
        bfs_queue.push(node->GetLowKeyPair().second);
        auto current_node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);
        for (KeyNodePointerPair *element_p = current_node->Begin(); element_p != current_node->End(); element_p++) {
          bfs_queue.push(element_p->second);
        }
      }
    }

    size_t heap_size = 0;
    while (!all_nodes.empty()) {
      BaseNode *node = all_nodes.front();
      all_nodes.pop();
      if (node->GetType() != NodeType::LeafType) {
        auto current_node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);
        // TODO(rohanagg): Little bit wrong
        heap_size += sizeof(BaseNode) + current_node->GetSize() * sizeof(KeyNodePointerPair);
      } else {
        auto current_node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node);
        heap_size += sizeof(BaseNode) + current_node->GetSize() * sizeof(KeyValuePair);
        for (KeyValuePair *element_p = current_node->Begin(); element_p != current_node->End(); element_p++) {
          if (element_p->second != NULL) {
            // destroy the list of values in a key
            heap_size += element_p->second->size() * sizeof(ValueType);
          }
        }
      }
    }
    return heap_size;
  }

  /*
    Insert - adds element in the tree
    The structure followed in the code is the lowKeyPointerPair's pointer represents
    the leftmost pointer. While for all other nodes their pointer go to a node on their
    right, ie containing values with keys greater than them.
  */

  bool Insert(const KeyElementPair element, std::function<bool(const ValueType)> predicate) {
    /* If root is NULL then we make a Leaf Node.
     */
    common::SpinLatch::ScopedSpinLatch guard(&root_latch_);

    if (root_ == nullptr) {
      KeyNodePointerPair p1, p2;
      p1.first = element.first;
      p2.first = element.first;
      p1.second = NULL;
      p2.second = NULL;
      root_ = ElasticNode<KeyValuePair>::Get(leaf_node_size_upper_threshold_, NodeType::LeafType, 0,
                                             leaf_node_size_upper_threshold_, p1, p2);
    }

    BaseNode *current_node = root_;
    // Stack of pointers
    std::vector<BaseNode *> node_list;

    // Traversing Down and maintaining a stack of pointers
    while (current_node->GetType() != NodeType::LeafType) {
      node_list.push_back(current_node);
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = node->FindLocation(element.first, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      if (index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      } else
        current_node = node->GetLowKeyPair().second;
    }

    bool finished_insertion = false;
    // We maintain the element that we have to recursively insert up.
    // This is the element that has to be inserted into the inner nodes.
    KeyNodePointerPair inner_node_element;
    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);

    auto location_greater_key_leaf = node->FindLocation(element.first, this);
    if (location_greater_key_leaf != node->Begin()) {
      if (KeyCmpEqual((location_greater_key_leaf - 1)->first, element.first)) {
        auto itr_list = (location_greater_key_leaf - 1)->second->begin();
        while (itr_list != (location_greater_key_leaf - 1)->second->end()) {
          if (ValueCmpEqual(*itr_list, element.second) || predicate(*itr_list)) {
            return false;
          }
          itr_list++;
        }
        (location_greater_key_leaf - 1)->second->push_back(element.second);
        finished_insertion = true;
      }
    }
    if (!finished_insertion) {
      auto value_list = new std::list<ValueType>();
      value_list->push_back(element.second);
      KeyValuePair key_list_value;
      key_list_value.first = element.first;
      key_list_value.second = value_list;
      if (node->InsertElementIfPossible(key_list_value, node->FindLocation(element.first, this))) {
        // If you can directly insert in the leaf - Insertion is over
        finished_insertion = true;
      } else {
        // Otherwise you split the node
        // Now pay attention to the fact that when we copy up the right pointer
        // remains same and the new node after splitting that gets returned to us
        // becomes the right pointer.
        // Thus our inner node element will contain the first key of the splitted
        // Node and a pointer to the splitted node.
        auto splitted_node = node->SplitNode();
        auto splitted_node_begin = splitted_node->Begin();
        // To decide which leaf to put in the element
        if (KeyCmpGreater(splitted_node_begin->first, element.first)) {
          node->InsertElementIfPossible(key_list_value, node->FindLocation(element.first, this));
        } else {
          splitted_node->InsertElementIfPossible(key_list_value, splitted_node->FindLocation(element.first, this));
        }

        // Set the siblings correctly
        // node_next = node_right
        // node_next_left = splitted
        // splitted_right = node_right
        // node_right = splitted
        // splitted_left = node
        if (node->GetElasticHighKeyPair()->second != NULL) {
          auto node_next = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node->GetElasticHighKeyPair()->second);
          node_next->GetElasticLowKeyPair()->second = splitted_node;
        }
        splitted_node->GetElasticHighKeyPair()->second = node->GetElasticHighKeyPair()->second;
        node->GetElasticHighKeyPair()->second = splitted_node;
        splitted_node->GetElasticLowKeyPair()->second = node;

        // Set the inner node that needs to be passed to the parents
        inner_node_element.first = splitted_node->Begin()->first;
        inner_node_element.second = splitted_node;
      }
    }

    while (!finished_insertion && !node_list.empty()) {
      auto inner_node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(*node_list.rbegin());
      node_list.pop_back();

      // If we can insert element now without splitting then we are done
      if (inner_node->InsertElementIfPossible(inner_node_element,
                                              inner_node->FindLocation(inner_node_element.first, this))) {
        finished_insertion = true;
      } else {
        // otherwise we have to recursively split again

        /*TODO: Some problem here - the leftmost pointer is not set properly
        Have to add code to set it properly*/
        auto splitted_node = inner_node->SplitNode();
        auto splitted_node_begin = splitted_node->Begin();
        if (KeyCmpGreater(splitted_node_begin->first, inner_node_element.first)) {
          inner_node->InsertElementIfPossible(inner_node_element,
                                              inner_node->FindLocation(inner_node_element.first, this));
        } else {
          splitted_node->InsertElementIfPossible(inner_node_element,
                                                 splitted_node->FindLocation(inner_node_element.first, this));
        }

        splitted_node->GetElasticLowKeyPair()->second = splitted_node->Begin()->second;
        inner_node_element.first = splitted_node->Begin()->first;
        inner_node_element.second = splitted_node;
        splitted_node->PopBegin();
      }
    }

    // If still insertion is not finished we have to split the root node.
    // Remember the root must have been split by now.
    if (!finished_insertion) {
      auto old_root = root_;
      KeyNodePointerPair p1, p2;
      p1.first = inner_node_element.first; /*This is a dummy initialization*/
      p2.first = inner_node_element.first; /*This is a dummy initialization*/
      p1.second = old_root;                /*This initialization matters*/
      p2.second = NULL;                    /*This is a dummy initialization*/
      root_ = ElasticNode<KeyNodePointerPair>::Get(inner_node_size_upper_threshold_, NodeType::InnerType,
                                                   root_->GetDepth() + 1, inner_node_size_upper_threshold_, p1, p2);
      auto new_root_node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(root_);
      new_root_node->InsertElementIfPossible(inner_node_element,
                                             new_root_node->FindLocation(inner_node_element.first, this));
    }
    return true;
  }

  /*
   *    InsertUnique - adds key and value in the tree only
   *    if the particular value has not been seen for that
   *    particular key.
   */
  //  bool InsertUnique(KeyType key, ValueType value) {
  //    // retrieve the value list of the key from tree
  //    std::list<ValueType> * value_list_p = FindValueOfKey(key);
  //    if(value_list_p == NULL) {  // list doesn't exist, insert safely
  //      auto value_list = new std::list<ValueType>();
  //      value_list->push_back(value);
  //      Insert(KeyValuePair(key, value_list));
  //    }
  //    else { // check if the list does not have it
  //      // return if there is the value
  //      auto itr = value_list_p->begin();
  //      for(;itr!=value_list_p->end();itr++) {
  //        if(ValueCmpEqual(*itr, value)) {
  //          return false;
  //        }
  //      }
  //      value_list_p->push_back(value);
  //    }
  //    return true;
  //  }

  explicit BPlusTree(KeyComparator p_key_cmp_obj = KeyComparator{},
                     KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{}, KeyHashFunc p_key_hash_obj = KeyHashFunc{},
                     ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{},
                     ValueHashFunc p_value_hash_obj = ValueHashFunc{})
      : BPlusTreeBase(),
        // Key comparator, equality checker and hasher
        key_cmp_obj_{p_key_cmp_obj},
        key_eq_obj_{p_key_eq_obj},
        key_hash_obj_{p_key_hash_obj},

        // Value equality checker and hasher
        value_eq_obj_{p_value_eq_obj},
        value_hash_obj_{p_value_hash_obj},

        // key-node ID pair cmp, equality checker and hasher
        key_node_id_pair_cmp_obj_{this},
        key_node_id_pair_eq_obj_{this},

        // key-value pair cmp, equality checker and hasher
        key_value_pair_cmp_obj_{this},
        key_value_pair_eq_obj_{this},
        root_(nullptr) {}

  /*
   * Destructor - Destroy BplusTree instance
   *
   */
  ~BPlusTree() { FreeTree(); }
};  // class BPlusTree

}  // namespace terrier::storage::index
