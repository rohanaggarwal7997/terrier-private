#include "test_util/test_harness.h"
#include "storage/index/bplustree.h"
#include "storage/storage_defs.h"

namespace terrier::storage::index {

struct BPlusTreeTests : public TerrierTest {};


// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, BasicTreeClassTest) {

 	auto bwtree = new BPlusTree<int, TupleSlot>;
 	BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
 	BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;
 	// Get inner Node
 	auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);
 	std::cout<<node->Begin()->first<<std::endl;
 	p1.first = 3;
 	node->PushBack(p1);
 	std::cout<<node->Begin()->first;
 	delete bwtree;
}
}  // namespace terrier::storage::index
