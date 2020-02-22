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
  BPlusTreeBase() :{}

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
  class KeyNodeIDPairComparator;
  class KeyNodeIDPairHashFunc;
  class KeyNodeIDPairEqualityChecker;
  class KeyValuePairHashFunc;
  class KeyValuePairEqualityChecker;

  // KeyType-NodeID pair
  using KeyNodeIDPair = std::pair<KeyType, NodeID>;
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
   * class KeyNodeIDPairComparator - Compares key-value pair for < relation
   *
   * Only key values are compares. However, we should use WrappedKeyComparator
   * instead of the raw one, since there could be -Inf involved in inner nodes
   */
  class KeyNodeIDPairComparator {
   public:
    const KeyComparator *key_cmp_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyNodeIDPairComparator() = delete;

    /*
     * Constructor - Initialize a key-NodeID pair comparator using
     *               wrapped key comparator
     */
    KeyNodeIDPairComparator(BPlusTree *p_tree_p) : key_cmp_obj_p{&p_tree_p->key_cmp_obj} {}

    /*
     * operator() - Compares whether a key NodeID pair is less than another
     *
     * We only compare keys since there should not be duplicated
     * keys inside an inner node
     */
    inline bool operator()(const KeyNodeIDPair &knp1, const KeyNodeIDPair &knp2) const {
      // First compare keys for relation
      return (*key_cmp_obj_p)(knp1.first, knp2.first);
    }
  };

  /*
   * class KeyNodeIDPairEqualityChecker - Checks KeyNodeIDPair equality
   *
   * Only keys are checked since there should not be duplicated keys inside
   * inner nodes. However we should always use wrapped key eq checker rather
   * than wrapped raw key eq checker
   */
  class KeyNodeIDPairEqualityChecker {
   public:
    const KeyEqualityChecker *key_eq_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyNodeIDPairEqualityChecker() = delete;

    /*
     * Constructor - Initialize a key node pair eq checker
     */
    KeyNodeIDPairEqualityChecker(BPlusTree *p_tree_p) : key_eq_obj_p{&p_tree_p->key_eq_obj} {}

    /*
     * operator() - Compares key-NodeID pair by comparing keys
     */
    inline bool operator()(const KeyNodeIDPair &knp1, const KeyNodeIDPair &knp2) const {
      return (*key_eq_obj_p)(knp1.first, knp2.first);
    }
  };

  /*
   * class KeyNodeIDPairHashFunc - Hashes a key-NodeID pair into size_t
   */
  class KeyNodeIDPairHashFunc {
   public:
    const KeyHashFunc *key_hash_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyNodeIDPairHashFunc() = delete;

    /*
     * Constructor - Initialize a key value pair hash function
     */
    KeyNodeIDPairHashFunc(BPlusTree *p_tree_p) : key_hash_obj_p{&p_tree_p->key_hash_obj} {}

    /*
     * operator() - Hashes a key-value pair by hashing each part and
     *              combine them into one size_t
     *
     * We use XOR to combine hashes of the key and value together into one
     * single hash value
     */
    inline size_t operator()(const KeyNodeIDPair &knp) const { return (*key_hash_obj_p)(knp.first); }
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
    // the low key pointer always points to a KeyNodeIDPair structure
    // inside the base node, which is either the first element of the
    // node sep list (InnerNode), or a class member (LeafNode)
    const KeyNodeIDPair *low_key_p;

    // high key points to the KeyNodeIDPair inside the LeafNode and InnerNode
    // if there is neither SplitNode nor MergeNode. Otherwise it
    // points to the item inside split node or merge right sibling branch
    const KeyNodeIDPair *high_key_p;

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
    NodeMetaData(const KeyNodeIDPair *p_low_key_p, const KeyNodeIDPair *p_high_key_p, NodeType p_type, int p_depth,
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
    BaseNode(NodeType p_type, const KeyNodeIDPair *p_low_key_p, const KeyNodeIDPair *p_high_key_p, int p_depth,
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
    inline const KeyNodeIDPair &GetHighKeyPair() const { return *metadata.high_key_p; }

    /*
     * GetLowKeyPair() - Returns the pointer to low key node id pair
     *
     * The return value is nullptr for LeafNode and its delta chain
     */
    inline const KeyNodeIDPair &GetLowKeyPair() const { return *metadata.low_key_p; }

    /*
     * GetNextNodeID() - Returns the next NodeID of the current node
     */
    inline NodeID GetNextNodeID() const { return metadata.high_key_p->second; }

    /*
     * GetLowKeyNodeID() - Returns the NodeID for low key
     *
     * NOTE: This function should not be called for leaf nodes
     * since the low key node ID for leaf node is not defined
     */
    inline NodeID GetLowKeyNodeID() const {
      TERRIER_ASSERT(!IsOnLeafDeltaChain(), "This should not be called on leaf nodes.");

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
    inline void SetLowKeyPair(const KeyNodeIDPair *p_low_key_p) { metadata.low_key_p = p_low_key_p; }

    /*
     * SetHighKeyPair() - Sets the high key pair of metdtata
     */
    inline void SetHighKeyPair(const KeyNodeIDPair *p_high_key_p) { metadata.high_key_p = p_high_key_p; }
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
    KeyNodeIDPair low_key;
    KeyNodeIDPair high_key;

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
    ElasticNode(NodeType p_type, int p_depth, int p_item_count, const KeyNodeIDPair &p_low_key,
                const KeyNodeIDPair &p_high_key)
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
     * Destroy() - Frees the memory by calling AllocationMeta::Destroy()
     *
     * Note that function does not call destructor, and instead the destructor
     * should be called first before this function is called
     *
     * Note that this function does not call destructor for the class since
     * it holds multiple instances of tree nodes, we should call destructor
     * for each individual type outside of this class, and only frees memory
     * when Destroy() is called.
     */
    void Destroy() const {
      // This finds the allocation header for this base node, and then
      // traverses the linked list
      ElasticNode::GetAllocationHeader(this)->Destroy();
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
                                   const KeyNodeIDPair &p_low_key, const KeyNodeIDPair &p_high_key) {
      // Currently this is always true - if we want a larger array then
      // just remove this line
      TERRIER_ASSERT(size == p_item_count, "Remove this if you want a larger array.");

      // Allocte memory for
      //   1. AllocationMeta (chunk)
      //   2. node meta
      //   3. ElementType array
      // basic template + ElementType element size * (node size) + CHUNK_SIZE()
      // Note: do not make it constant since it is going to be modified
      // after being returned
      auto *alloc_base = new char[sizeof(ElasticNode) + size * sizeof(ElementType) + AllocationMeta::CHUNK_SIZE()];
      TERRIER_ASSERT(alloc_base != nullptr, "Allocation failed.");

      // Initialize the AllocationMeta - tail points to the first byte inside
      // class ElasticNode; limit points to the first byte after class
      // AllocationMeta
      new (reinterpret_cast<AllocationMeta *>(alloc_base))
          AllocationMeta{alloc_base + AllocationMeta::CHUNK_SIZE(), alloc_base + sizeof(AllocationMeta)};

      // The first CHUNK_SIZE() byte is used by class AllocationMeta
      // and chunk data
      auto *node_p = reinterpret_cast<ElasticNode *>(alloc_base + AllocationMeta::CHUNK_SIZE());

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
    static ElasticNode *GetNodeHeader(const KeyNodeIDPair *low_key_p) {
      static constexpr size_t low_key_offset = offsetof(ElasticNode, low_key);

      return reinterpret_cast<ElasticNode *>(reinterpret_cast<uint64_t>(low_key_p) - low_key_offset);
    }

    /*
     * GetAllocationHeader() - Returns the address of class AllocationHeader
     *                         embedded inside the ElasticNode object
     */
    static AllocationMeta *GetAllocationHeader(const ElasticNode *node_p) {
      return reinterpret_cast<AllocationMeta *>(reinterpret_cast<uint64_t>(node_p) - AllocationMeta::CHUNK_SIZE());
    }

    /*
     * InlineAllocate() - Allocates a delta node in preallocated area preceeds
     *                    the data area of this ElasticNode
     *
     * Note that for any given NodeType, we always know its low key and the
     * low key always points to the struct inside base node. This way, we
     * compute the offset of the low key from the begining of the struct,
     * and then subtract it with CHUNK_SIZE() to derive the address of
     * class AllocationMeta
     *
     * Note that since this function is accessed when the header is unknown
     * so (1) it is static, and (2) it takes low key p which is universally
     * available for all node type (stored in NodeMetadata)
     */
    static void *InlineAllocate(const KeyNodeIDPair *low_key_p, size_t size) {
      const ElasticNode *node_p = GetNodeHeader(low_key_p);
      TERRIER_ASSERT(&node_p->low_key == low_key_p, "low_key is not low_key_p.");

      // Jump over chunk content
      AllocationMeta *meta_p = GetAllocationHeader(node_p);

      void *p = meta_p->Allocate(size);
      TERRIER_ASSERT(p != nullptr, "Allocation failed.");

      return p;
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
  class InnerNode : public ElasticNode<KeyNodeIDPair> {
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
    ~InnerNode() { this->~ElasticNode<KeyNodeIDPair>(); }

    /*
     * GetSplitSibling() - Split InnerNode into two halves.
     *
     * This function does not change the current node since all existing nodes
     * should be read-only to avoid data race. It copies half of the inner node
     * into the split sibling, and return the sibling node.
     */
    InnerNode *GetSplitSibling() const {
      // Call function in class ElasticNode to determine the size of the
      // inner node
      int key_num = this->GetSize();

      // Inner node size must be > 2 to avoid empty split node
      TERRIER_ASSERT(key_num >= 2, "Inner node size must be > 2 to avoid empty split node");

      // Same reason as in leaf node - since we only split inner node
      // without a delta chain on top of it, the sep list size must equal
      // the recorded item count
      TERRIER_ASSERT(key_num == this->GetItemCount(), "the sep list size must equal the recorded item count");

      int split_item_index = key_num / 2;

      // This is the split point of the inner node
      auto copy_start_it = this->Begin() + split_item_index;

      // We need this to allocate enough space for the embedded array
      auto sibling_size = static_cast<int>(std::distance(copy_start_it, this->End()));

      // This sets metadata inside BaseNode by calling SetMetaData()
      // inside inner node constructor
      auto *inner_node_p = reinterpret_cast<InnerNode *>(ElasticNode<KeyNodeIDPair>::Get(
          sibling_size, NodeType::InnerType, 0, sibling_size, this->At(split_item_index), this->GetHighKeyPair()));

      // Call overloaded PushBack() to insert an array of elements
      inner_node_p->PushBack(copy_start_it, this->End());

      // Since we copy exactly that many elements
      TERRIER_ASSERT(inner_node_p->GetSize() == sibling_size, "Copied number of elements must match.");
      TERRIER_ASSERT(inner_node_p->GetSize() == inner_node_p->GetItemCount(), "Copied number of elements must match.");

      return inner_node_p;
    }
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

    /*
     * FindSplitPoint() - Find the split point that could divide the node
     *                    into two even siblings
     *
     * If such point does not exist then we manage to find a point that
     * divides the node into two halves that are as even as possible (i.e.
     * make the size difference as small as possible)
     *
     * This function works by first finding the key on the exact central
     * position, after which it scans forward to find a KeyValuePair
     * with a different key. If this fails then it scans backwards to find
     * a KeyValuePair with a different key.
     *
     * NOTE: If both split points would make an uneven division with one of
     * the node size below the merge threshold, then we do not split,
     * and return -1 instead. Otherwise the index of the spliting point
     * is returned
     */
    int FindSplitPoint(const BPlusTree *t) const {
      int central_index = this->GetSize() / 2;
      TERRIER_ASSERT(central_index > 1, "Index out of range.");

      // This will used as upper_bound and lower_bound key
      const KeyValuePair &central_kvp = this->At(central_index);

      // Move it to the element before data_list
      auto it = this->Begin() + central_index - 1;

      // If iterator has reached the begin then we know there could not
      // be any split points
      while ((it != this->Begin()) && t->KeyCmpEqual(it->first, central_kvp.first)) {
        it--;
      }

      // This is the real split point
      it++;

      // This size is exactly the index of the split point
      int left_sibling_size = std::distance(this->Begin(), it);

      if (left_sibling_size > static_cast<int>(t->GetLeafNodeSizeLowerThreshold())) {
        return left_sibling_size;
      }

      // Move it to the element after data_list
      it = this->Begin() + central_index + 1;

      // If iterator has reached the end then we know there could not
      // be any split points
      while ((it != this->End()) && t->KeyCmpEqual(it->first, central_kvp.first)) {
        it++;
      }

      int right_sibling_size = std::distance(it, this->End());

      if (right_sibling_size > static_cast<int>(t->GetLeafNodeSizeLowerThreshold())) {
        return std::distance(this->Begin(), it);
      }

      return -1;
    }

    /*
     * GetSplitSibling() - Split the node into two halves
     *
     * Although key-values are stored as independent pairs, we always split
     * on the point such that no keys are separated on two leaf nodes
     * This decision is made to make searching easier since now we could
     * just do a binary search on the base page to decide whether a
     * given key-value pair exists or not
     *
     * This function splits a leaf node into halves based on key rather
     * than items. This implies the number of keys would be even, but no
     * guarantee is made about the number of key-value items, which might
     * be very unbalanced, cauing node size varing much.
     *
     * NOTE: This function allocates memory, and if it is not used
     * e.g. by a CAS failure, then caller needs to delete it
     *
     * NOTE 2: Split key is stored in the low key of the new leaf node
     *
     * NOTE 3: This function assumes no out-of-bound key, i.e. all keys
     * stored in the leaf node are < high key. This is valid since we
     * already filtered out those key >= high key in consolidation.
     *
     * NOTE 4: On failure of split (i.e. could not find a split key that evenly
     * or almost evenly divide the leaf node) then the return value of this
     * function is nullptr
     */
    LeafNode *GetSplitSibling(const BPlusTree *t) const {
      // When we split a leaf node, it is certain that there is no delta
      // chain on top of it. As a result, the number of items must equal
      // the actual size of the data list
      TERRIER_ASSERT(this->GetSize() == this->GetItemCount(),
                     "the number of items does not equal the actual size of the data list");

      // This is the index of the actual key-value pair in data_list
      // We need to substract this value from the prefix sum in the new
      // inner node
      int split_item_index = FindSplitPoint(t);

      // Could not split because we could not find a split point
      // and the caller is responsible for not spliting the node
      // This should happen relative infrequently, in a sense that
      // oversized page would affect performance
      if (split_item_index == -1) {
        return nullptr;
      }

      // This is an iterator pointing to the split point in the vector
      // note that std::advance() operates efficiently on std::vector's
      // RandomAccessIterator
      auto copy_start_it = this->Begin() + split_item_index;

      // This is the end point for later copy of data
      auto copy_end_it = this->End();

      // This is the key part of the key-value pair, also the low key
      // of the new node and new high key of the current node (will be
      // reflected in split delta later in its caller)
      const KeyType &split_key = copy_start_it->first;

      auto sibling_size = static_cast<int>(std::distance(copy_start_it, copy_end_it));

      // This will call SetMetaData inside its constructor
      auto *leaf_node_p = reinterpret_cast<LeafNode *>(
          ElasticNode<KeyValuePair>::Get(sibling_size, NodeType::LeafType, 0, sibling_size,
                                         std::make_pair(split_key, ~INVALID_NODE_ID), this->GetHighKeyPair()));

      // Copy data item into the new node using PushBack()
      leaf_node_p->PushBack(copy_start_it, copy_end_it);

      TERRIER_ASSERT(leaf_node_p->GetSize() == sibling_size, "Copied number of elements must match.");
      TERRIER_ASSERT(leaf_node_p->GetSize() == leaf_node_p->GetItemCount(), "Copied number of elements must match.");

      return leaf_node_p;
    }
  };

};  // class BPlusTree

}  // namespace terrier::storage::index
