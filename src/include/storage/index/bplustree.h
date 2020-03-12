#pragma once

#include <cstring>
#include <functional>
#include <iostream>
#include <queue>
#include <set>
#include <list>
#include <unordered_map>
#include <unordered_set>

#include "common/shared_latch.h"
#include "storage/index/index.h"
#include "storage/index/index_defs.h"


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
  BPlusTreeBase() {}

  /*
   * Destructor
   */
  ~BPlusTreeBase() {}
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
    const KeyComparator *key_cmp_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyNodePointerPairComparator() = delete;

    /*
     * Constructor - Initialize a key-NodeID pair comparator using
     *               wrapped key comparator
     */
    KeyNodePointerPairComparator(BPlusTree *p_tree_p) : key_cmp_obj_p{&p_tree_p->key_cmp_obj} {}

    /*
     * operator() - Compares whether a key NodeID pair is less than another
     *
     * We only compare keys since there should not be duplicated
     * keys inside an inner node
     */
    inline bool operator()(const KeyNodePointerPair &knp1, const KeyNodePointerPair &knp2) const {
      // First compare keys for relation
      return (*key_cmp_obj_p)(knp1.first, knp2.first);
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
    const KeyEqualityChecker *key_eq_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyNodePointerPairEqualityChecker() = delete;

    /*
     * Constructor - Initialize a key node pair eq checker
     */
    KeyNodePointerPairEqualityChecker(BPlusTree *p_tree_p) : key_eq_obj_p{&p_tree_p->key_eq_obj} {}

    /*
     * operator() - Compares key-NodeID pair by comparing keys
     */
    inline bool operator()(const KeyNodePointerPair &knp1, const KeyNodePointerPair &knp2) const {
      return (*key_eq_obj_p)(knp1.first, knp2.first);
    }
  };

  /*
   * class KeyNodePointerPairHashFunc - Hashes a key-NodeID pair into size_t
   */
  class KeyNodePointerPairHashFunc {
   public:
    const KeyHashFunc *key_hash_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyNodePointerPairHashFunc() = delete;

    /*
     * Constructor - Initialize a key value pair hash function
     */
    KeyNodePointerPairHashFunc(BPlusTree *p_tree_p) : key_hash_obj_p{&p_tree_p->key_hash_obj} {}

    /*
     * operator() - Hashes a key-value pair by hashing each part and
     *              combine them into one size_t
     *
     * We use XOR to combine hashes of the key and value together into one
     * single hash value
     */
    inline size_t operator()(const KeyNodePointerPair &knp) const { return (*key_hash_obj_p)(knp.first); }
  };

  ///////////////////////////////////////////////////////////////////
  // Comparator, equality checker and hasher for key-value pair
  ///////////////////////////////////////////////////////////////////

  /*
   * class KeyValuePairComparator - Comparator class for KeyValuePair
   */
  class KeyValuePairComparator {
   public:
    const KeyComparator *key_cmp_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyValuePairComparator() = delete;

    /*
     * Constructor
     */
    KeyValuePairComparator(BPlusTree *p_tree_p) : key_cmp_obj_p{&p_tree_p->key_cmp_obj} {}

    /*
     * operator() - Compares key-value pair by comparing each component
     *              of them
     *
     * NOTE: This function only compares keys with KeyType. For +/-Inf
     * the wrapped raw key comparator will fail
     */
    inline bool operator()(const KeyValuePair &kvp1, const KeyValuePair &kvp2) const {
      return (*key_cmp_obj_p)(kvp1.first, kvp2.first);
    }
  };

  /*
   * class KeyValuePairEqualityChecker - Checks KeyValuePair equality
   */
  class KeyValuePairEqualityChecker {
   public:
    const KeyEqualityChecker *key_eq_obj_p;
    const ValueEqualityChecker *value_eq_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyValuePairEqualityChecker() = delete;

    /*
     * Constructor - Initialize a key value pair equality checker with
     *               WrappedKeyEqualityChecker and ValueEqualityChecker
     */
    KeyValuePairEqualityChecker(BPlusTree *p_tree_p)
        : key_eq_obj_p{&p_tree_p->key_eq_obj}, value_eq_obj_p{&p_tree_p->value_eq_obj} {}

    /*
     * operator() - Compares key-value pair by comparing each component
     *              of them
     *
     * NOTE: This function only compares keys with KeyType. For +/-Inf
     * the wrapped raw key comparator will fail
     */
    inline bool operator()(const KeyValuePair &kvp1, const KeyValuePair &kvp2) const {
      return ((*key_eq_obj_p)(kvp1.first, kvp2.first)) && ((*value_eq_obj_p)(kvp1.second, kvp2.second));
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
  inline bool KeyCmpLess(const KeyType &key1, const KeyType &key2) const { return key_cmp_obj(key1, key2); }

  /*
   * KeyCmpEqual() - Compare a pair of keys for equality
   *
   * This functions compares keys for equality relation
   */
  inline bool KeyCmpEqual(const KeyType &key1, const KeyType &key2) const { return key_eq_obj(key1, key2); }

  /*
   * KeyCmpGreaterEqual() - Compare a pair of keys for >= relation
   *
   * It negates result of keyCmpLess()
   */
  inline bool KeyCmpGreaterEqual(const KeyType &key1, const KeyType &key2) const { return !KeyCmpLess(key1, key2); }

  /*
   * KeyCmpGreater() - Compare a pair of keys for > relation
   *
   * It flips input for keyCmpLess()
   */
  inline bool KeyCmpGreater(const KeyType &key1, const KeyType &key2) const { return KeyCmpLess(key2, key1); }

  /*
   * KeyCmpLessEqual() - Compare a pair of keys for <= relation
   */
  inline bool KeyCmpLessEqual(const KeyType &key1, const KeyType &key2) const { return !KeyCmpGreater(key1, key2); }

  ///////////////////////////////////////////////////////////////////
  // Value Comparison Member
  ///////////////////////////////////////////////////////////////////

  /*
   * ValueCmpEqual() - Compares whether two values are equal
   */
  inline bool ValueCmpEqual(const ValueType &v1, const ValueType &v2) { return value_eq_obj(v1, v2); }


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
    const KeyNodePointerPair *low_key_p;

    // high key points to the KeyNodePointerPair inside the LeafNode and InnerNode
    // if there is neither SplitNode nor MergeNode. Otherwise it
    // points to the item inside split node or merge right sibling branch
    const KeyNodePointerPair *high_key_p;

    // The type of the node; this is forced to be represented as a short type
    NodeType type;

    // This is the height of the node
    short depth;

    // This counts the total number of items in the node
    int item_count;

    // Latch for each node
    common::SharedLatch node_latch_;

    /*
     * Constructor
     */
    NodeMetaData(const KeyNodePointerPair *p_low_key_p, const KeyNodePointerPair *p_high_key_p, NodeType p_type, int p_depth,
                 int p_item_count)
        : low_key_p{p_low_key_p},
          high_key_p{p_high_key_p},
          type{p_type},
          depth{static_cast<short>(p_depth)},
          item_count{p_item_count} {}
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
    NodeMetaData metadata;

   public:
    /*
     * Constructor - Initialize type and metadata
     */
    BaseNode(NodeType p_type, const KeyNodePointerPair *p_low_key_p, const KeyNodePointerPair *p_high_key_p, int p_depth,
             int p_item_count)
        : metadata{p_low_key_p, p_high_key_p, p_type, p_depth, p_item_count} {}

    /*
     * Type() - Return the type of node
     *
     * This method does not allow overridding
     */
    inline NodeType GetType() const { return metadata.type; }

    /*
     * GetNodeMetaData() - Returns a const reference to node metadata
     *
     * Please do not override this method
     */
    inline const NodeMetaData &GetNodeMetaData() const { return metadata; }

    /*
     * IsInnerNode() - Returns true if the node is an inner node
     *
     * This is useful if we want to collect all seps on an inner node
     * If the top of delta chain is an inner node then just do not collect
     * and use the node directly
     */
    inline bool IsInnerNode() const { return GetType() == NodeType::InnerType; }

     /*
     * GetLowKey() - Returns the low key of the current base node
     *
     * NOTE: Since it is defined that for LeafNode the low key is undefined
     * and pointers should be set to nullptr, accessing the low key of
     * a leaf node would result in Segmentation Fault
     */
    inline const KeyType &GetLowKey() const { return metadata.low_key_p->first; }

    /*
     * GetHighKey() - Returns a reference to the high key of current node
     *
     * This function could be called for all node types including leaf nodes
     * and inner nodes.
     */
    inline const KeyType &GetHighKey() const { return metadata.high_key_p->first; }

    /*
     * GetHighKeyPair() - Returns the pointer to high key node id pair
     */
    inline const KeyNodePointerPair &GetHighKeyPair() const { return *metadata.high_key_p; }

    /*
     * GetLowKeyPair() - Returns the pointer to low key node id pair
     *
     * The return value is nullptr for LeafNode and its delta chain
     */
    inline const KeyNodePointerPair &GetLowKeyPair() const { return *metadata.low_key_p; }

    /*
     * GetNextNodeID() - Returns the next NodeID of the current node
     */
    inline BaseNode * GetNextNodeID() const { return metadata.high_key_p->second; }

    /*
     * GetLowKeyNodeID() - Returns the NodeID for low key
     *
     * NOTE: This function should not be called for leaf nodes
     * since the low key node ID for leaf node is not defined
     */
    inline BaseNode * GetLowKeyNodeID() const {
      return metadata.low_key_p->second;
    }

    /*
     * GetDepth() - Returns the Height of the Node
     */
    inline int GetDepth() const { return metadata.depth; }

    /*
     * GetItemCount() - Returns the item count of the current node
     */
    inline int GetItemCount() const { return metadata.item_count; }

    /*
     * GetNodeExclusiveLatch() - Obtain the exclusive lock to the current node
     */
    void GetNodeExclusiveLatch() { metadata.node_latch_.LockExclusive(); }

    /*
     * GetNodeSharedLatch() - Obtain the Shared lock to the current node
     */
    void GetNodeSharedLatch() { metadata.node_latch_.LockShared(); }

    /*
     * ReleaseNodeLatch() - Release the lock to the current node
     */
    void ReleaseNodeLatch() { metadata.node_latch_.Unlock(); }

    /*
     * TryExclusiveLock() - Try to get the exclusive lock
     */
    bool TryExclusiveLock() { return metadata.node_latch_.TryExclusiveLock(); }

    /*
     * TrySharedLock() - Try to get the shared read lock
     */
    bool TrySharedLock() { return metadata.node_latch_.TryLockShared(); }

    /*
     * SetLowKeyPair() - Sets the low key pair of metadata
     */
    inline void SetLowKeyPair(const KeyNodePointerPair *p_low_key_p) { metadata.low_key_p = p_low_key_p; }

    /*
     * SetHighKeyPair() - Sets the high key pair of metdtata
     */
    inline void SetHighKeyPair(const KeyNodePointerPair *p_high_key_p) { metadata.high_key_p = p_high_key_p; }

    /*
      SetType() - Sets the type of metadata
    */
    inline void SetType(const NodeType input_type) {metadata.type = input_type;}

    /*
      SetDepth() - Sets the depth of metadata
    */
    inline void SetDepth(const short input_depth) {metadata.depth = input_depth;}

    /*
      SetItemCount() - Sets the item_count of metadata
    */
    inline void SetItemCount(const int input_item_count) {metadata.item_count = input_item_count;}
  };

  /*
   * class ElasticNode - The base class for elastic node types, i.e. InnerNode
   *                     and LeafNode
   *
   * Since for InnerNode and LeafNode, the number of elements is not a compile
   * time known constant. However, for efficient tree traversal we must inline
   * all elements to reduce cache misses with workload that's less predictable
   */
  template <typename ElementType>
  class ElasticNode : public BaseNode {
   private:
    // These two are the low key and high key of the node respectively
    // since we could not add it in the inherited class (will clash with
    // the array which is invisible to the compiler) so they must be added here
    KeyNodePointerPair low_key;
    KeyNodePointerPair high_key;

    // This is the end of the elastic array
    // We explicitly store it here to avoid calculating the end of the array
    // everytime
    ElementType *end;

    // This is the starting point
    ElementType start[0];

   public:
    /*
     * Constructor
     *
     * Note that this constructor uses the low key and high key stored as
     * members to initialize the NodeMetadata object in class BaseNode
     */
    ElasticNode(NodeType p_type, int p_depth, int p_item_count, const KeyNodePointerPair &p_low_key,
                const KeyNodePointerPair &p_high_key)
        : BaseNode{p_type, &low_key, &high_key, p_depth, p_item_count},
          low_key{p_low_key},
          high_key{p_high_key},
          end{start} {}

    /*
     * Copy() - Copy constructs another instance
     */
    static ElasticNode *Copy(const ElasticNode &other) {
      ElasticNode *node_p = ElasticNode::Get(other.GetItemCount(), other.GetType(), other.GetDepth(),
                                             other.GetItemCount(), other.GetLowKeyPair(), other.GetHighKeyPair());

      node_p->PushBack(other.Begin(), other.End());

      return node_p;
    }

    /*
      Free elastic node
    */
    void FreeElasticNode() {
      // for (ElementType *element_p = Begin(); element_p != End(); element_p++) {
      //   // Manually calls destructor when the node is destroyed
      //   element_p->~ElementType();
      // }
      ElasticNode *beginningAllocation = this;
      delete[] reinterpret_cast<char *>(beginningAllocation); 
    }

    /*
     IntializeEnd() - Make end = start
    */
    inline void InitializeEnd() {end = start;}

    /*
     SetEnd() - Make end = start + offset
    */
    inline void SetEnd(int offset) {
      end = start + offset;
    }

    /*
     * Begin() - Returns a begin iterator to its internal array
     */
    inline ElementType *Begin() { return start; }

    /*
     * RBegin() - Returns a reverse begin iterator to its internal array
     */
    inline ElementType *RBegin() {
      if (this->GetSize() == 0) {
        return NULL;
      } else {
        return (end-1);
      }
    }

    inline const ElementType *Begin() const { return start; }

    /*
     * End() - Returns an end iterator that is similar to the one for vector
     */
    inline ElementType *End() { return end; }

    inline const ElementType *End() const { return end; }

    /*
     * REnd() - Returns the element before the first element
     *
     * Note that since we returned an invalid pointer into the array, the
     * return value should not be modified and is therefore of const type
     */
    inline const ElementType *REnd() { return start - 1; }

    inline const ElementType *REnd() const { return start - 1; }

    /*
     * PushBack() - Push back an element
     *
     * This function takes an element type and copy-construct it on the array
     * which is invisible to the compiler. Therefore we must call placement
     * operator new to do the job
     */
    inline void PushBack(const ElementType &element) {
      // Placement new + copy constructor using end pointer
      new (end) ElementType{element};

      // Move it pointing to the enxt available slot, if not reached the end
      end++;
    }

    /*
     * PushBack() - Push back a series of elements
     *
     * The overloaded PushBack() could also push an array of elements
     */
    inline void PushBack(const ElementType *copy_start_p, const ElementType *copy_end_p) {
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
        std::cout << element_p->first << " ";
      }
    }
    /*
    insertElementIfPossible - Returns true if inserted and false if node full
    Inserts at location provided.
    */
    bool InsertElementIfPossible(const ElementType &element, ElementType *location) {
      if(GetSize() >= this->GetItemCount()) return false;
      if(end - location > 0)
      std::memmove(reinterpret_cast<void *>(location + 1), reinterpret_cast<void *>(location),
        (end - location)*sizeof(ElementType));
      new (location) ElementType{element};
      end = end + 1;
      return true;
    }

    /*
    SplitNode - Splits the current node and returns a pointer to the new node. 
    Returns NULL if node is not already full
    */
    ElasticNode * SplitNode() {
      if(this->GetSize() < this->GetItemCount()) return NULL;
      ElasticNode * new_node = this->Get(this->GetSize(), this->GetType(), this->GetDepth(),
        this->GetItemCount(), *this->GetElasticLowKeyPair(), *this->GetElasticHighKeyPair());
      ElementType * copy_from_location = Begin() + ((this->GetSize()) / 2);
      // Can be memcopy
      std::memmove(reinterpret_cast<void *>(new_node->Begin()), reinterpret_cast<void *>(copy_from_location),
        (end - copy_from_location)*sizeof(ElementType));
      new_node->SetEnd((end - copy_from_location));
      end = copy_from_location;
      return new_node;
    }

    /*
     MergeNode - Merge a given node's entries to the current node.
     Returns true if merge successful, false otherwise
     */
    bool MergeNode(ElasticNode * next_node) {
      // Merge the right type
      if (this->GetType() != next_node->GetType()) {
        return false;
      }

      // Is Merging possible
      if ((this->GetItemCount() - this->GetSize()) < next_node->GetSize()) {
        return false;
      }

      std::memmove(reinterpret_cast<void *>(this->End()),
        reinterpret_cast<void *>(next_node->Begin()), (next_node->GetSize())*sizeof(ElementType));
      SetEnd(this->GetSize() + next_node->GetSize());
      return true;
    }

    /*
    Pop Begin - Pops the first element from the list
    Returns False if empty
    */
    bool PopBegin() {
      if(this->GetSize() == 0) return false;
      if(this->GetSize() == 1) {
        SetEnd(0);
        return true;
      }
      std::memmove(reinterpret_cast<void *>(start), reinterpret_cast<void *>(start + 1),
        (this->GetSize() - 1)*sizeof(ElementType));
      SetEnd(this->GetSize() - 1);
      return true;
    }

    /*
    Pop Back - Pops the last element from the list
    Returns False if empty
    */
    bool PopEnd() {
      if(this->GetSize() == 0) return false;
      if(this->GetSize() == 1) {
        SetEnd(0);
        return true;
      }
      SetEnd(this->GetSize() - 1);
      return true;
    }

    /*
    Erase - remove the element at index 'i' from the list
    Returns False if empty
    */
    bool Erase(int i) {

      if(this->GetSize() <= i) return false;
      
      if(this->GetSize() == 1) {
        SetEnd(0);
        return true;
      }
      std::memmove(reinterpret_cast<void *>(start + i), reinterpret_cast<void *>(start + i + 1),
        (this->GetSize() - i - 1)*sizeof(ElementType));
      SetEnd(this->GetSize() - 1);
      return true;
    }    

    /*
    FindLocation - Returns the start of the first element that compares
    greater to the key provided
    */
    ElementType * FindLocation(const KeyType &element, BPlusTree * tree) {

      ElementType * iter = Begin();
      while((iter != end) &&
        (!tree->KeyCmpGreater(iter->first, element))) iter++;
      return iter;
    }



    /*
     * GetSize() - Returns the size of the embedded list
     *
     * Note that the return type is integer since we use integer to represent
     * the size of a node
     */
    inline int GetSize() const { return static_cast<int>(End() - Begin()); }
     /*
     * SetElasticLowKeyPair() - Sets the low key of Elastic Node
     */
    inline void SetElasticLowKeyPair(const KeyNodePointerPair &p_low_key) { low_key = p_low_key; }

    /*
     * SetElasticHighKeyPair() - Sets the high key of Elastic Node
     */
    inline void SetElasticHighKeyPair(const KeyNodePointerPair &p_high_key) { high_key = p_high_key; }

    /*
     * GetElasticLowKeyPair() - Returns a pointer to the low key current Elastic node
     */
    inline KeyNodePointerPair *GetElasticLowKeyPair() { return &low_key; }

    /*
     * GetElasticHighKeyPair() - Returns a pointer to the high key of current Elastic node
     */
    inline KeyNodePointerPair *GetElasticHighKeyPair() { return &high_key; }

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
    inline static ElasticNode *Get(int size,  // Number of elements
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
      // auto elastic_node = reinterpret_cast<ElasticNode *>(alloc_base);
      // elastic_node->SetLowKeyPair(elastic_node->GetElasticLowKeyPair());
      // elastic_node->SetHighKeyPair(elastic_node->GetElasticHighKeyPair());
      // elastic_node->SetItemCount(p_item_count);
      // elastic_node->SetType(p_type);
      // elastic_node->SetDepth(p_depth);
      // elastic_node->SetElasticLowKeyPair(p_low_key);
      // elastic_node->SetElasticHighKeyPair(p_high_key);
      // elastic_node->InitializeEnd();

      auto elastic_node = reinterpret_cast<ElasticNode *>(alloc_base);
      new (elastic_node) ElasticNode{p_type, p_depth, p_item_count, p_low_key, p_high_key};


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
    inline ElementType &At(const int index) {
      // The index must be inside the valid range
      TERRIER_ASSERT(index < GetSize(), "Index out of range.");

      return *(Begin() + index);
    }

    inline const ElementType &At(const int index) const {
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
  const KeyComparator key_cmp_obj;

  // Raw key eq checker
  const KeyEqualityChecker key_eq_obj;

  // Raw key hasher
  const KeyHashFunc key_hash_obj;

  // Check whether values are equivalent
  const ValueEqualityChecker value_eq_obj;

  // Hash ValueType into a size_t
  const ValueHashFunc value_hash_obj;

  // The following three are used for std::pair<KeyType, NodeID>
  const KeyNodePointerPairComparator key_node_id_pair_cmp_obj;
  const KeyNodePointerPairEqualityChecker key_node_id_pair_eq_obj;

  // The following two are used for hashing KeyValuePair
  const KeyValuePairComparator key_value_pair_cmp_obj;
  const KeyValuePairEqualityChecker key_value_pair_eq_obj;

  private:
  BaseNode * root;
  common::SharedLatch root_latch;

  public:

  /*
    Get Root - Returns the current root
  */
  inline BaseNode * GetRoot() {return root;}

  /*
    Get Element
  */
  inline KeyElementPair GetElement(KeyType key, ValueType value) {
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
    if(root == NULL) {
      return false;
    }

    BaseNode * current_node = root;

    // Traversing Down to the right leaf node
    while(current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = node->FindLocation(key, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      if(index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      }
      else current_node = node->GetLowKeyPair().second;
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    for (KeyValuePair * element_p = node->Begin();
      element_p!=node->End(); element_p ++) {
      if(element_p->first == key) {
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
  void FindValueOfKey(KeyType key, std::vector<ValueType>& result) {

    root_latch.LockShared();

    if(root == NULL) {
      root_latch.Unlock();
      return;
    }

    BaseNode * current_node = root;
    BaseNode * parent = NULL;

    /*
        Locking Code
    */
    current_node->GetNodeSharedLatch();
    root_latch.Unlock();
    /*
      Locking Code End
    */


    // Traversing Down to the right leaf node
    while(current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = node->FindLocation(key, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      parent = current_node;
      if(index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      }
      else current_node = node->GetLowKeyPair().second;


      /*
        Locking Code
      */
      current_node->GetNodeSharedLatch();
      parent->ReleaseNodeLatch();
      /*
        Locking Code End
      */
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    for (KeyValuePair * element_p = node->Begin();
         element_p!=node->End(); element_p ++) {
      if(KeyCmpEqual(element_p->first,key)) {
        auto itr_list = element_p->second->begin();
        while(itr_list != element_p->second->end()) {
          result.push_back(*itr_list);
          itr_list++;
        }

        current_node->ReleaseNodeLatch();
        return;
      }
    }

    current_node->ReleaseNodeLatch();
  }

  void PrintTreeNode(BaseNode* node) {
    if(node->GetType() != NodeType::LeafType) {
      auto node_elastic = 
        reinterpret_cast<ElasticNode<KeyValuePair> *>(node);
      node_elastic->PrintElasticNode();
    } else {
      auto node_elastic = 
        reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);  
      node_elastic->PrintElasticNode();
    }
    std::cout << '\t';
  }

  /*
    Traverses Down the root in a BFS manner and prints all the nodes
  */
  void PrintTree() {
    if(root == NULL) return;
    std::queue<BaseNode *> bfs_queue;
    std::queue<BaseNode *> all_nodes;
    bfs_queue.push(root);

    // print root
    std::cout << "Printing Root " << std::endl;
    if(root->GetType() != NodeType::LeafType) {
      auto root_elastic = 
        reinterpret_cast<ElasticNode<KeyValuePair> *>(root);
      root_elastic->PrintElasticNode();
    } else {
      auto root_elastic = 
        reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(root);  
      root_elastic->PrintElasticNode();
    }

    while(!bfs_queue.empty()) {
      BaseNode * node = bfs_queue.front();
      bfs_queue.pop();
      all_nodes.push(node);
      if(node->GetType() != NodeType::LeafType) {
        std::cout << "printing children " << std::endl;

        bfs_queue.push(node->GetLowKeyPair().second);
        PrintTreeNode(node->GetLowKeyPair().second);

        auto current_node = 
          reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);
        for (KeyNodePointerPair * element_p = current_node->Begin();
          element_p!=current_node->End(); element_p ++) {
          bfs_queue.push(element_p->second);
          PrintTreeNode(element_p->second);
        }
      }
    }
    std::cout << std::endl;
  }

  /*
    Traverses Down the root in a BFS manner and frees all the nodes
  */
  void FreeTree() {
    if(root == NULL) return;
    std::queue<BaseNode *> bfs_queue;
    std::queue<BaseNode *> all_nodes;
    bfs_queue.push(root);

    while(!bfs_queue.empty()) {
      BaseNode * node = bfs_queue.front();
      bfs_queue.pop();
      all_nodes.push(node);
      if(node->GetType() != NodeType::LeafType) {
        bfs_queue.push(node->GetLowKeyPair().second);
        auto current_node = 
          reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);
        for (KeyNodePointerPair * element_p = current_node->Begin();
          element_p!=current_node->End(); element_p ++) {
          bfs_queue.push(element_p->second);
        }
      }
    }

    while(!all_nodes.empty()) {
      BaseNode * node = all_nodes.front();
      all_nodes.pop();
      if(node->GetType() != NodeType::LeafType) {
        auto current_node = 
          reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);
        current_node->FreeElasticNode();
      } else {
        auto current_node = 
          reinterpret_cast<ElasticNode<KeyValuePair> *>(node);
        for (KeyValuePair * element_p = current_node->Begin();
             element_p!=current_node->End(); element_p ++) {
          if(element_p->second != NULL) {
            // destroy the list of values in a key
            delete element_p->second;
          }
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
    BaseNode * current_node) {

    bool return_answer = true;
    if(current_node->GetType() == NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
      for (KeyValuePair * element_p = node->Begin();
        element_p!=node->End(); element_p ++) {
        /* All Keys within Range*/
        if(this->KeyCmpGreater(low_key, element_p->first) ||
          this->KeyCmpGreater(element_p->first, high_key)) {
          return false;
        }
        /* Size of Node is correct */
        if(current_node != root)
        if(node->GetSize() < leaf_node_size_lower_threshold_ ||
          node->GetSize() > leaf_node_size_upper_threshold_) {
          return false;
        }
        keys.erase(element_p->first);
      }
    } else {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      /* Size of Node is correct */
      if(current_node != root)
      if(node->GetSize() < inner_node_size_lower_threshold_ || 
        node->GetSize() > inner_node_size_upper_threshold_) {
        return false;
      }
      return_answer &= this->StructuralIntegrityVerification(low_key, node->Begin()->first, keys,
        current_node->GetLowKeyPair().second);

      KeyNodePointerPair * iter = node->Begin();
      while((iter + 1) != node->End()) {
        low_key = iter->first; 
        return_answer &= this->StructuralIntegrityVerification(low_key, (iter + 1)->first, keys,
        iter->second);
        iter++;
      }

      return_answer &= this->StructuralIntegrityVerification(iter->first, high_key, keys,
        iter->second);
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
    KeyType key = *keys.begin();
    if(root == NULL) {
      return false;
    }

    BaseNode * current_node = root;

    // Traversing Down to the right leaf node
    while(current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = node->FindLocation(key, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      if(index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      }
      else current_node = node->GetLowKeyPair().second;
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    auto itr = keys.begin();
    while(node != NULL) {
      // iterate over all the values in the leaf node checking with the key set
      for (KeyValuePair * element_p = node->Begin();
           element_p!=node->End(); element_p++) {
        if(element_p->first != *itr) {
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
    KeyType key = *keys.rbegin();
    if(root == NULL) {
      return false;
    }

    BaseNode * current_node = root;

    // Traversing Down to the right leaf node
    while(current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = node->FindLocation(key, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      if(index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      }
      else current_node = node->GetLowKeyPair().second;
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    auto itr = keys.rbegin();
    while(node != NULL) {
      // iterate over all the values in the leaf node checking with the key set
      for (KeyValuePair * element_p = node->End()-1;
           element_p!=node->REnd(); element_p--) {
        if(element_p->first != *itr) {
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
  bool DuplicateKeyValuesCheck(std::unordered_map<KeyType, std::set<ValueType> >& keys_values) {
    auto itr = keys_values.begin();
    for(;itr!=keys_values.end();itr++) {
      KeyType k = itr->first;
      std::set<ValueType> values = keys_values[k];
      std::vector<ValueType> result;
      FindValueOfKey(k, result);
      if(result.size() == 0) {
        return false;
      }
      for(auto it = result.begin(); it != result.end(); it++) {
        if(values.count(*(it)) == 0) {
          return false;
        } else {
          values.erase(*(it));
        }
      }
      if (values.size() != 0) {
        return false;
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
  bool DuplicateKeyValueUniqueInsertCheck(std::unordered_map<KeyType, std::vector<ValueType> >& keys_values) {
    auto itr = keys_values.begin();
    for(;itr!=keys_values.end();itr++) {
      KeyType k = itr->first;
      std::vector<ValueType> values = keys_values[k];
      std::list<ValueType> * values_list_p = FindValueOfKey(k);
      if(values_list_p == NULL) {
        return false;
      }
      auto it_list = values_list_p->begin();
      for(unsigned j = 0; j != values.size(); j++) {
        if (it_list == values_list_p->end()) {
          return false;
        }
        if(values[j] != *(it_list)) {
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

    root_latch.LockExclusive();
    if(root == NULL) return 0;

    std::queue<BaseNode *> bfs_queue;
    std::queue<BaseNode *> all_nodes;
    bfs_queue.push(root);

    while(!bfs_queue.empty()) {
      BaseNode * node = bfs_queue.front();
      bfs_queue.pop();
      all_nodes.push(node);
      if(node->GetType() != NodeType::LeafType) {
        bfs_queue.push(node->GetLowKeyPair().second);
        auto current_node = 
          reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);
        for (KeyNodePointerPair * element_p = current_node->Begin();
          element_p!=current_node->End(); element_p ++) {
          bfs_queue.push(element_p->second);
        }
      }
    }

    size_t heap_size = 0;
    while(!all_nodes.empty()) {
      BaseNode * node = all_nodes.front();
      all_nodes.pop();
      if(node->GetType() != NodeType::LeafType) {
        auto current_node = 
          reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);
        // TODO: Little bit wrong
        heap_size += sizeof(BaseNode) + current_node->GetSize() * sizeof(KeyNodePointerPair);
      } else {
        auto current_node = 
          reinterpret_cast<ElasticNode<KeyValuePair> *>(node);
        heap_size += sizeof(BaseNode) + current_node->GetSize() * sizeof(KeyValuePair);
        for (KeyValuePair * element_p = current_node->Begin();
             element_p!=current_node->End(); element_p ++) {
          if(element_p->second != NULL) {
            // destroy the list of values in a key
            heap_size += element_p->second->size() * sizeof(ValueType);
          }
        }
      }
    }
    return heap_size;
  }


  /*
    Scan Ascending - Duplicates the Scan Ascending Behaviour in bwtree_index.h
  */
  void ScanAscending(KeyType index_low_key, KeyType index_high_key, bool low_key_exists, uint32_t num_attrs,
    bool high_key_exists, uint32_t limit, std::vector<TupleSlot> *value_list, const IndexMetadata *metadata) {
    
    root_latch.LockExclusive();

    if(root == NULL) {
      root_latch.Unlock();
      return;
    }

    BaseNode * current_node = root;
    BaseNode * parent = NULL;
    /*
        Locking Code
    */
    current_node->GetNodeSharedLatch();
    root_latch.Unlock();
    /*
      Locking Code End
    */

    // Traversing Down to the correct leaf node
    while(current_node->GetType() != NodeType::LeafType) {

      // Set parent for releasing the lock
      parent = current_node;

      if(low_key_exists) {
        auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
        // Note that Find Location returns the location of first element
        // that compare greater than
        auto index_pointer = node->FindLocation(index_low_key, this);
        // Thus we have to go in the left side of location which will be the
        // pointer of the previous location.

        if(index_pointer != node->Begin()) {
          index_pointer -= 1;
          current_node = index_pointer->second;
        }
        else current_node = node->GetLowKeyPair().second;
      } else {
        current_node = current_node->GetLowKeyPair().second;
      }
      /*
        Locking Code
      */
      current_node->GetNodeSharedLatch();
      parent->ReleaseNodeLatch();
      /*
        Locking Code End
      */
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    KeyValuePair * element_p;
    if(low_key_exists) {
      element_p = node->FindLocation(index_low_key, this);
      if(element_p != node->Begin()) {
        if(KeyCmpEqual((element_p - 1)->first, index_low_key)) {
          element_p --;
        }
      } 
    } else {
      element_p = node->Begin();
    }

    while ((limit == 0 || value_list->size() < limit) &&
      (!high_key_exists || element_p->first.PartialLessThan(index_high_key, metadata, num_attrs))) {
      
      auto itr_list = element_p->second->begin();
      while(itr_list != element_p->second->end()) {
        value_list->push_back(*itr_list);
        if(!(limit == 0 || value_list->size() < limit)) break;
        itr_list++;
      }

      element_p++;
      if(element_p == node->End()) {
        if(node->GetHighKeyPair().second == NULL) break;
        parent = node;
        node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node->GetHighKeyPair().second);
        current_node = node;
        /*
        Locking Code
        */
        current_node->GetNodeSharedLatch();
        parent->ReleaseNodeLatch();
        /*
          Locking Code End
        */
        element_p = node->Begin();
      }
    }

    current_node->ReleaseNodeLatch();

    return;
  }

  /*
    Scan Descending - Duplicates the Scan Descending Behaviour in bwtree_index.h
  */
  bool ScanDescending(KeyType index_low_key, KeyType index_high_key, std::vector<TupleSlot> *value_list) {

    root_latch.LockExclusive();

    if(root == NULL) {
      root_latch.Unlock();
      return true;
    }

    BaseNode * current_node = root;
    BaseNode * parent = NULL;
    /*
        Locking Code
    */
    current_node->GetNodeSharedLatch();
    root_latch.Unlock();
    /*
      Locking Code End
    */

    // Traversing Down to the right leaf node
    while(current_node->GetType() != NodeType::LeafType) {

        auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
        // Note that Find Location returns the location of first element
        // that compare greater than
        auto index_pointer = node->FindLocation(index_high_key, this);
        // Thus we have to go in the left side of location which will be the
        // pointer of the previous location.

        // set parent for crabbing
        parent = node;

        if(index_pointer != node->Begin()) {
          index_pointer -= 1;
          current_node = index_pointer->second;
        }
        else current_node = node->GetLowKeyPair().second;
        /*
        Locking Code
        */
        current_node->GetNodeSharedLatch();
        parent->ReleaseNodeLatch();
        /*
        Locking Code End
        */
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    KeyValuePair * element_p;
    element_p = node->FindLocation(index_high_key, this);
    if(element_p != node->Begin()) {
      // if(KeyCmpEqual((element_p - 1)->first, index_low_key)) {
      element_p --;
      // }
    } else {
      if(node->GetLowKeyPair().second == NULL) {
        current_node->ReleaseNodeLatch();
        return true;
      } else {
        parent = node;
        node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node->GetLowKeyPair().second);
        current_node = node;
        if (!(current_node->TrySharedLock())) {
          parent->ReleaseNodeLatch();
          return false;
        }
        parent->ReleaseNodeLatch();
        element_p = node->End() - 1;
      }
    }

    while(KeyCmpGreaterEqual(element_p->first, index_low_key)) {

      auto itr_list = element_p->second->begin();
      while(itr_list != element_p->second->end()) {

        value_list->push_back(*itr_list);
        itr_list++;
      }

      element_p--;
      if(element_p == node->Begin() - 1) {
        parent = node;
        if(node->GetLowKeyPair().second == NULL) break;
        node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node->GetLowKeyPair().second);
        current_node = node;
        if (!(current_node->TrySharedLock())) {
          parent->ReleaseNodeLatch();
          return false;
        }
        parent->ReleaseNodeLatch();
        element_p = node->End() - 1;
      }
    }

    current_node->ReleaseNodeLatch();
    return true;
  }

  /*
    Scan Limit Descending - Duplicates the Scan Limit Descending Behaviour in bwtree_index.h
  */
  bool ScanLimitDescending(KeyType index_low_key, KeyType index_high_key, std::vector<TupleSlot> *value_list,
    uint32_t limit) {

    root_latch.LockExclusive();

    if(root == NULL) {
      root_latch.Unlock();
      return true;
    }

    BaseNode * current_node = root;
    BaseNode * parent = NULL;
    /*
        Locking Code
    */
    current_node->GetNodeSharedLatch();
    root_latch.Unlock();
    /*
      Locking Code End
    */

    // Traversing Down to the right leaf node
    while(current_node->GetType() != NodeType::LeafType) {

        auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
        // Note that Find Location returns the location of first element
        // that compare greater than
        auto index_pointer = node->FindLocation(index_high_key, this);
        // Thus we have to go in the left side of location which will be the
        // pointer of the previous location.

        // set parent for crabbing
        parent = node;
        if(index_pointer != node->Begin()) {
          index_pointer -= 1;
          current_node = index_pointer->second;
        }
        else current_node = node->GetLowKeyPair().second;

        /*
        Locking Code
        */
        current_node->GetNodeSharedLatch();
        parent->ReleaseNodeLatch();
        /*
        Locking Code End
        */
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    KeyValuePair * element_p;
    element_p = node->FindLocation(index_high_key, this);
    if(element_p != node->Begin()) {
      // if(KeyCmpEqual((element_p - 1)->first, index_low_key)) {
      element_p --;
      // }
    } else {
      if(node->GetLowKeyPair().second == NULL) {
        current_node->ReleaseNodeLatch();
        return true;
      } else {
        parent = node;
        node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node->GetLowKeyPair().second);
        current_node = node;
        if (!(current_node->TrySharedLock())) {
          parent->ReleaseNodeLatch();
          return false;
        }
        parent->ReleaseNodeLatch();
        element_p = node->End() - 1;
      }
    }

    while((value_list->size() < limit) && KeyCmpGreaterEqual(element_p->first, index_low_key)) {

      auto itr_list = element_p->second->begin();
      while(itr_list != element_p->second->end()) {
        value_list->push_back(*itr_list);
        if(value_list->size() >= limit) break;
        itr_list++;
      }

      element_p--;
      if(element_p == node->Begin() - 1) {
        parent = node;
        if(node->GetLowKeyPair().second == NULL) break;
        node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node->GetLowKeyPair().second);
        current_node = node;
        if (!(current_node->TrySharedLock())) {
          parent->ReleaseNodeLatch();
          return false;
        }
        parent->ReleaseNodeLatch();
        element_p = node->End() - 1;
      }
    }

    current_node->ReleaseNodeLatch();
    return true;
  }

  bool ReleaseAllLocks(std::vector<BaseNode *> &node_list, bool got_root_latch) {
    while(!node_list.empty()) {
      (*node_list.rbegin())->ReleaseNodeLatch();
      node_list.pop_back();
    }
    if(got_root_latch) {
      got_root_latch = false;
      root_latch.Unlock();
    }
    return got_root_latch;
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
    root_latch.LockExclusive();
    bool got_root_latch = true;

    if (root == NULL) {
      KeyNodePointerPair p1, p2;
      p1.first = element.first;
      p2.first = element.first;
      p1.second = NULL;
      p2.second = NULL;
      root = ElasticNode<KeyValuePair>::Get(leaf_node_size_upper_threshold_, NodeType::LeafType, 0,
                                            leaf_node_size_upper_threshold_, p1, p2);
    }


    BaseNode *current_node = root;


    /*
      Locking Code
    */
      current_node->GetNodeExclusiveLatch();
    /*
      Locking Code End
    */


    // Stack of pointers
    std::vector<BaseNode *> node_list;

    // Traversing Down and maintaining a stack of pointers
    while (current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);


      /*
        Locking Code
      */
      if(node->GetSize() < node->GetItemCount()) {
        got_root_latch = ReleaseAllLocks(node_list, got_root_latch);
      } 
      /*
        Locking Code End
      */




      node_list.push_back(current_node);
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


      /*
        Locking Code
      */
      current_node->GetNodeExclusiveLatch();
      /*
        Locking Code End
      */
    }



    bool finished_insertion = false;
    // We maintain the element that we have to recursively insert up.
    // This is the element that has to be inserted into the inner nodes.
    KeyNodePointerPair inner_node_element;
    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);

    auto location_greater_key_leaf = node->FindLocation(element.first, this);
    if (location_greater_key_leaf != node->Begin()) {
      if(KeyCmpEqual((location_greater_key_leaf - 1)->first,element.first)) {

        auto itr_list = (location_greater_key_leaf - 1)->second->begin();
        while(itr_list != (location_greater_key_leaf - 1)->second->end()) {
          if(ValueCmpEqual(*itr_list, element.second) || predicate(*itr_list)) {

            /*
              Release all locks if the value is already present
            */
            current_node->ReleaseNodeLatch();
            got_root_latch = ReleaseAllLocks(node_list, got_root_latch);

            /*
              Locking Code End
  
            */
            return false;
          }
          itr_list++;
        }
        (location_greater_key_leaf - 1)->second->push_back(element.second);

        /*
        Locking Code
        */
        current_node->ReleaseNodeLatch();
        got_root_latch = ReleaseAllLocks(node_list, got_root_latch);
        /*
          Locking Code End
        */

        finished_insertion = true;
      }  
    } 
    if(finished_insertion == false)
    {
      auto value_list = new std::list<ValueType>();
      value_list->push_back(element.second);
      KeyValuePair key_list_value;
      key_list_value.first = element.first;
      key_list_value.second = value_list;
      if (node->InsertElementIfPossible(key_list_value, node->FindLocation(element.first, this))) {
        // If you can directly insert in the leaf - Insertion is over
        finished_insertion = true;
        got_root_latch = ReleaseAllLocks(node_list, got_root_latch);
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

      current_node->ReleaseNodeLatch();
    }

    while(!finished_insertion && node_list.size() > 0) {
      auto inner_node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(*node_list.rbegin());
      node_list.pop_back();

      // If we can insert element now without splitting then we are done
      if(inner_node->InsertElementIfPossible(inner_node_element,
        inner_node->FindLocation(inner_node_element.first, this))) {
        finished_insertion = true;
      } else {
        // otherwise we have to recursively split again

        auto splitted_node = inner_node->SplitNode();
        auto splitted_node_begin = splitted_node->Begin();
        if(KeyCmpGreater(splitted_node_begin->first, inner_node_element.first)) {
          inner_node->InsertElementIfPossible(inner_node_element,
            inner_node->FindLocation(inner_node_element.first, this));
        } else {
          splitted_node->InsertElementIfPossible(inner_node_element,
            splitted_node->FindLocation(inner_node_element.first, this));
        }

        splitted_node->GetElasticLowKeyPair()->second =
          splitted_node->Begin()->second;
        inner_node_element.first = splitted_node->Begin()->first;
        inner_node_element.second = splitted_node;
        splitted_node->PopBegin();
      }
      inner_node->ReleaseNodeLatch();    
    }


    // If still insertion is not finished we have to split the root node.
    // Remember the root must have been split by now.
    if(!finished_insertion) {
      auto old_root = root;
      KeyNodePointerPair p1, p2;
      p1.first = inner_node_element.first; /*This is a dummy initialization*/
      p2.first = inner_node_element.first; /*This is a dummy initialization*/
      p1.second = old_root;                 /*This initialization matters*/
      p2.second = NULL; /*This is a dummy initialization*/
      root = ElasticNode<KeyNodePointerPair>::Get(inner_node_size_upper_threshold_,
                                   NodeType::InnerType, root->GetDepth() + 1,
                                   inner_node_size_upper_threshold_,
                                   p1, p2); 
      auto new_root_node =
        reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(root);
      new_root_node->InsertElementIfPossible(inner_node_element,
      new_root_node->FindLocation(inner_node_element.first, this));
    }

    if(got_root_latch) {
      got_root_latch = false;
      root_latch.Unlock();
    }

    return true;
  }

  template<typename ElementType> 
  void DeleteRebalance(ElasticNode<KeyNodePointerPair> * parent, 
    BaseNode * input_child_pointer, int index, int node_lower_threshold) {

    auto child = reinterpret_cast<ElasticNode<ElementType> *>(input_child_pointer);

    if (child->GetSize() >= node_lower_threshold) {
      return;
    } 

    // Need to rebalance
    if (index > -1) {
      ElasticNode<ElementType> * left_sibling;
      if(index == 0) {
        left_sibling = reinterpret_cast<ElasticNode<ElementType> *>(parent->GetLowKeyPair().second);
      } else {
        left_sibling = reinterpret_cast<ElasticNode<ElementType> *>((parent->Begin() + index - 1)->second);
      }

      if(left_sibling->GetSize() > node_lower_threshold) {


      /*
      This is for Inner Node only
      Remember for leaf node - You cannot bring down A.
      Capital chars represent Keys. Small chars represent nodes.                    
                          A                                               C
                        /   \                                          /     \
                     [ B ]     [ C  D ]           ==                [B  A]    [D]                 
                      /  \      /  \  \                             /  \  \   /  \
                     a    b    c   d   e                           a   b   c  d   e

          Initially
         C - (parent->Begin() + index)-key
         A - left sibling->Rbegin()->first
         D - child->Begin()->first
         d - child->lowkeypair().second
         c - left sibling->Rbegin()->second

          Finally
         A - (parent->Begin() + index)-key
         C - child->Begin()->first
         c - child->lowkeypair().second
         d - child->Begin()->second
         D - (child->Begin() + 1)->first - Happens automatically
      */

        // Borrow one

        if(child->GetType() == NodeType::LeafType) {
          (parent->Begin() + index)->first = left_sibling->RBegin()->first; 
          child->InsertElementIfPossible(*(left_sibling->RBegin()), child->Begin());
          left_sibling->PopEnd();
        } else {
          auto inner_child = reinterpret_cast<InnerNode *>(input_child_pointer);
          auto inner_left_sibling = reinterpret_cast<InnerNode *> (left_sibling);
          /* 
            Make C->d to insert in child
          */
          /*C*/auto parent_key = (parent->Begin() + index)->first;
          /*d*/auto current_low_pointer = inner_child->GetLowKeyPair().second; 
          KeyNodePointerPair to_insert;
          to_insert.first = parent_key;
          to_insert.second = current_low_pointer;
          inner_child->InsertElementIfPossible(to_insert, inner_child->Begin());
          /*
            Make low key pointer c
          */
          inner_child->GetElasticLowKeyPair()->second = inner_left_sibling->RBegin()->second;

          /*
            Update parent key to A
          */
          (parent->Begin() + index)->first = inner_left_sibling->RBegin()->first;

          /*
            Delete A->c
          */
          left_sibling->PopEnd();
        }
        return;
      }
    }

    if(index < parent->GetSize() - 1) {
      ElasticNode<ElementType> * right_sibling = 
        reinterpret_cast<ElasticNode<ElementType> *>((parent->Begin() + index + 1)->second);
      if(right_sibling->GetSize() > node_lower_threshold) {


        /*
        This is for Inner Node only
        Remember for leaf node - You cannot bring down A.
        Capital chars represent Keys. Small chars represent nodes.                    
                        X   A                                          X    C
                          \   \                                          /     \
                       [ B ]     [ C  D ]           ==                [B  A]    [D]                 
                        /  \      /  \  \                             /  \  \   /  \
                       a    b    c   d   e                           a   b   c  d   e

            Initially
           X - (parent->Begin() + index)-key
           A - (parent->Begin() + index + 1)-key
           C - right_sibling->Begin()->first
           c - right_sibling->lowkeypair().second
           d - right_sibling->Begin()->second
           D - (right_sibling->Begin() + 1)->first - Happens automatically

            Finally
           X - (parent->Begin() + index)-key
           C - (parent->Begin() + index + 1)-key
           A - child->Rbegin()->first
           D - right_sibling->Begin()->first
           d - right_sibling->lowkeypair().second
           c - child->Rbegin()->second
        */

        // Borrow one

        if(child->GetType() == NodeType::InnerType) {
          auto inner_child = reinterpret_cast<InnerNode *>(input_child_pointer);
          auto inner_right_sibling = reinterpret_cast<InnerNode *> (right_sibling);
          /* 
            Make A->c to insert in child
          */
          /*A*/auto parent_key = (parent->Begin() + index + 1)->first;
          /*c*/auto current_low_pointer = inner_right_sibling->GetLowKeyPair().second;
          KeyNodePointerPair to_insert;
          to_insert.first = parent_key;
          to_insert.second = current_low_pointer;
          inner_child->InsertElementIfPossible(to_insert, inner_child->End());
          /*
            Make low key pointer d
          */
          right_sibling->GetElasticLowKeyPair()->second = inner_right_sibling->Begin()->second;

          /*
            Update A to C
          */
          (parent->Begin() + index + 1)->first = inner_right_sibling->Begin()->first;

          /*
            Delete C-d
          */
          right_sibling->PopBegin();             
        } else {
          child->InsertElementIfPossible(*(right_sibling->Begin()), child->End());
          right_sibling->PopBegin();
          (parent->Begin() + index + 1)->first = right_sibling->Begin()->first; 
        }
        return;
      }
    }

    // Cannot redistribute, so we perform merge
    // We try to merge with left sibling first, if not possible 
    // merge with right sibling   
    if (index > -1) {
      ElasticNode<ElementType> * left_sibling;
      if(index == 0) {
        left_sibling = reinterpret_cast<ElasticNode<ElementType> *>(parent->GetLowKeyPair().second);
      } else {
        left_sibling = reinterpret_cast<ElasticNode<ElementType> *>((parent->Begin() + index - 1)->second);
      }

      /*
                           A                                         
                         /   \                                      B  A  C  D
                        B    [C  D]              ==                /  \  \  \  \
                       / \   /  \  \                              a    b  c  d  e
                      a   b  c   d  e
      */


      if(left_sibling->GetType() == NodeType::InnerType) {
        auto parent_key = (parent->Begin() + index)->first;
        auto current_low_pointer = child->GetLowKeyPair().second;
        auto inner_left_sibling = reinterpret_cast<InnerNode *> (left_sibling);

        KeyNodePointerPair to_insert;
        to_insert.first = parent_key;
        to_insert.second = current_low_pointer;
        inner_left_sibling->InsertElementIfPossible(to_insert, inner_left_sibling->End());
      }

      /*
      Fixing sibling pointers code
      */
      if(left_sibling->GetType() == NodeType::LeafType) {
        left_sibling->GetElasticHighKeyPair()->second = child->GetHighKeyPair().second;
        if(child->GetHighKeyPair().second != NULL)
        reinterpret_cast<ElasticNode<KeyValuePair> *>(child->GetHighKeyPair().second)
          ->GetElasticLowKeyPair()->second = left_sibling;
      }


      left_sibling->MergeNode(child);
      child->FreeElasticNode();
      parent->Erase(index);


    } else {
      ElasticNode<ElementType> * right_sibling = 
        reinterpret_cast<ElasticNode<ElementType> *>((parent->Begin() + index + 1)->second);

      if(right_sibling->GetType() == NodeType::InnerType) {
        auto parent_key = (parent->Begin() + index + 1)->first;
        auto current_low_pointer = right_sibling->GetLowKeyPair().second;
        auto inner_child = reinterpret_cast<InnerNode *> (child);

        KeyNodePointerPair to_insert;
        to_insert.first = parent_key;
        to_insert.second = current_low_pointer;
        inner_child->InsertElementIfPossible(to_insert, inner_child->End());
      }

      /*
      Fixing sibling pointers code
      */
      if(child->GetType() == NodeType::LeafType) {
        child->GetElasticHighKeyPair()->second = right_sibling->GetHighKeyPair().second;
        if(right_sibling->GetHighKeyPair().second != NULL)
        reinterpret_cast<ElasticNode<KeyValuePair> *>(right_sibling->GetHighKeyPair().second)
          ->GetElasticLowKeyPair()->second = child;
      }


      child->MergeNode(right_sibling);
      right_sibling->FreeElasticNode();
      parent->Erase(index+1);
    }

    return;
  }

  /*
  Delete with Lock
  Takes a coarse grained lock and calls the delete function
  */
  bool DeleteWithLock(const KeyElementPair &element) {
    root_latch.LockExclusive();
    bool is_deleted = Delete(root, element);
    root_latch.Unlock();
    return is_deleted;
  } 

  /*
   * Delete() - Remove a key-value pair from the tree
   *
   * This function returns false if the key and value pair does not
   * exist. Return true if delete succeeds
   *
   */
  bool Delete(BaseNode* current_node, const KeyElementPair &element) {
    // If tree is empty, return false
    if (current_node == NULL) {
      return false;
    }

    // If delete called on leaf node, just perform deletion
    // Else, call delete on child and check if child becomes underfull
    if (current_node->GetType() == NodeType:: LeafType) {
      // Leaf Node case => delete element
      auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
      auto leaf_position = node->FindLocation(element.first, this);
      if (leaf_position != node->Begin()) {
        leaf_position -= 1;
        if (KeyCmpEqual(leaf_position->first, element.first)) {

          bool element_present = false;
          auto itr_list = (leaf_position)->second->begin();
          while(itr_list != (leaf_position)->second->end()) {
            if(ValueCmpEqual(*itr_list, element.second)) {
              /*Delete element from list*/
              (leaf_position)->second->erase(itr_list);
              element_present = true;
              break;
            }
            itr_list++;
          }

          /*Not Found - Return false*/
          if(!element_present) {
            return false;
          }

          if(leaf_position->second->size() > 0) {
            return true;
          }

          /*If now the list is empty delete key-emptylist from the tree*/
          delete leaf_position->second;
          bool is_deleted = node->Erase(leaf_position - node->Begin());
          if (is_deleted && node->GetSize() == 0) {
            // all elements of tree are now deleted
            node->FreeElasticNode(); /*Important - we need to free node*/
            root = NULL;
          }
          return is_deleted;
        } else {
          return false;
        }
      } else {
        return false;
      }
    } else {
      // Inner Node case => call delete element on child and check if child becomes underfull
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      auto child_position = node->FindLocation(element.first, this);
      BaseNode* child_pointer;
      int index;
      if (child_position != node->Begin()) {
        child_pointer = (child_position - 1)->second;
        index = child_position - node->Begin() - 1;
      } else {
        child_pointer = node->GetLowKeyPair().second;
        index = -1;
      }
      bool is_deleted = Delete(child_pointer, element);

      // Now perform any rebalancing or merge on child if it becomes underfull
      if (is_deleted) {
        if (child_pointer->GetType() == NodeType:: LeafType) {
          DeleteRebalance<KeyValuePair>(node, child_pointer, 
            index, GetLeafNodeSizeLowerThreshold());
        } else {
          DeleteRebalance<KeyNodePointerPair>(node, child_pointer, 
            index, GetInnerNodeSizeLowerThreshold());
        }

        // Check if this node is root and if its size becomes 0
        if (node->GetSize() == 0) {
          root = current_node->GetLowKeyPair().second;
          node->FreeElasticNode();
        }

        return true;
      } else {
        return false;
      }
    }
  }

  BPlusTree(KeyComparator p_key_cmp_obj = KeyComparator{},
         KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{}, KeyHashFunc p_key_hash_obj = KeyHashFunc{},
         ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{}, ValueHashFunc p_value_hash_obj = ValueHashFunc{})
      : BPlusTreeBase(),
        // Key comparator, equality checker and hasher
        key_cmp_obj{p_key_cmp_obj},
        key_eq_obj{p_key_eq_obj},
        key_hash_obj{p_key_hash_obj},

        // Value equality checker and hasher
        value_eq_obj{p_value_eq_obj},
        value_hash_obj{p_value_hash_obj},

        // key-node ID pair cmp, equality checker and hasher
        key_node_id_pair_cmp_obj{this},
        key_node_id_pair_eq_obj{this},

        // key-value pair cmp, equality checker and hasher
        key_value_pair_cmp_obj{this},
        key_value_pair_eq_obj{this},
        root(NULL) {}


  /*
   * Destructor - Destroy BplusTree instance
   *
   */
  ~BPlusTree() {
    FreeTree();
  }
};  // class BPlusTree

}  // namespace terrier::storage::index
