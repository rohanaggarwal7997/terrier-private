#pragma once

#include <functional>

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

 private:


 protected:
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
  // KeyType-ValueType pair
  using KeyValuePair = std::pair<KeyType, ValueType>;

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

    // This is the depth of current delta chain
    // For merge delta node, the depth of the delta chain is the
    // sum of its two children
    short depth;

    // This counts the number of items alive inside the Node
    // when consolidating nodes, we use this piece of information
    // to reserve space for the new node
    int item_count;

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
     * GetDepth() - Returns the depth of the current node
     */
    inline int GetDepth() const { return metadata.depth; }

    /*
     * GetItemCount() - Returns the item count of the current node
     */
    inline int GetItemCount() const { return metadata.item_count; }

    /*
     * SetLowKeyPair() - Sets the low key pair of metadata
     */
    inline void SetLowKeyPair(const KeyNodePointerPair *p_low_key_p) { metadata.low_key_p = p_low_key_p; }

    /*
     * SetHighKeyPair() - Sets the high key pair of metdtata
     */
    inline void SetHighKeyPair(const KeyNodePointerPair *p_high_key_p) { metadata.high_key_p = p_high_key_p; }
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
     * Destructor
     *
     * All element types are destroyed inside the destruction function. D'tor
     * is called by Destroy(), and therefore should not be called directly
     * by external functions.
     *
     * Note that this is not called by Destroy() and instead it should be
     * called by an external function that destroies a delta chain, since in one
     * instance of thie class there might be multiple nodes of different types
     * so destroying should be dont individually with each type.
     */
    ~ElasticNode() {
      // Use two iterators to iterate through all existing elements
      for (ElementType *element_p = Begin(); element_p != End(); element_p++) {
        // Manually calls destructor when the node is destroyed
        element_p->~ElementType();
      }
    }

    /*
     * Begin() - Returns a begin iterator to its internal array
     */
    inline ElementType *Begin() { return start; }

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
     * GetSize() - Returns the size of the embedded list
     *
     * Note that the return type is integer since we use integer to represent
     * the size of a node
     */
    inline int GetSize() const { return static_cast<int>(End() - Begin()); }

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
      auto *node_p = reinterpret_cast<ElasticNode *>(alloc_base);

      // Call placement new to initialize all that could be initialized
      new (node_p) ElasticNode{p_type, p_depth, p_item_count, p_low_key, p_high_key};
      return node_p;
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

  public:
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
        key_value_pair_eq_obj{this} {}
};  // class BPlusTree

}  // namespace terrier::storage::index
