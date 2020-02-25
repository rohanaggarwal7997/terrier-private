#include "test_util/test_harness.h"
#include "storage/index/bplustree.h"
#include "storage/storage_defs.h"

#include <stdlib.h> 
#include <unordered_map>
#include <set>

namespace terrier::storage::index {

struct BPlusTreeTests : public TerrierTest {};

void BasicNodeInitializationInsertReadAndFreeTest(){
	auto bwtree = new BPlusTree<int, TupleSlot>;
 	BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
 	BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

 	// Get inner Node
 	auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);
 	
 	// To check if we can read what we inserted
 	std::vector<BPlusTree<int, TupleSlot>::KeyNodePointerPair> values;
 	for(unsigned i = 0; i < 10; i++) {
 		BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
 		p1.first = i;
 		values.push_back(p1);
 		node->PushBack(p1);
 		EXPECT_EQ(node->GetSize(), i+1);
 	}

 	using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;

 	unsigned i = 0;
 	for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
 		EXPECT_EQ(element_p->first, i);
 		i++;
 	}

 	// To Check if we are inserting at the correct place
 	EXPECT_EQ(reinterpret_cast<char *>(node) + 
 		sizeof(BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>>), 
 		reinterpret_cast<char *>(node->Begin()));

 	EXPECT_EQ(&(node->GetLowKeyPair()), node->GetElasticLowKeyPair());
 	EXPECT_EQ(&(node->GetHighKeyPair()), node->GetElasticHighKeyPair());
 	EXPECT_EQ(node->GetLowKeyPair().first, p1.first);
 	EXPECT_EQ(node->GetHighKeyPair().first, p2.first);
 	EXPECT_NE(&p1, &(node->GetLowKeyPair()));
 	EXPECT_NE(&p2, &(node->GetHighKeyPair()));

 	// Free the node - should not result in an ASAN
 	node->FreeElasticNode();
 	delete bwtree;

}

void InsertElementInNodeTest(){
	auto bwtree = new BPlusTree<int, TupleSlot>;
 	BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
 	BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

 	// Get inner Node
 	auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);
 	
 	for(unsigned i = 0; i < 10; i++) {
 		BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
 		p1.first = i;
 		EXPECT_EQ(node->InsertElementIfPossible(p1, node->Begin()), true);
 		EXPECT_EQ(node->GetSize(), i+1);
 	}

 	using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;

 	unsigned i = 9;
 	for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
 		EXPECT_EQ(element_p->first, i);
 		i--;
 	}

 	// To Check if we are inserting at the correct place
 	EXPECT_EQ(reinterpret_cast<char *>(node) + 
 		sizeof(BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>>), 
 		reinterpret_cast<char *>(node->Begin()));

 	EXPECT_EQ(&(node->GetLowKeyPair()), node->GetElasticLowKeyPair());
 	EXPECT_EQ(&(node->GetHighKeyPair()), node->GetElasticHighKeyPair());
 	EXPECT_EQ(node->GetLowKeyPair().first, p1.first);
 	EXPECT_EQ(node->GetHighKeyPair().first, p2.first);
 	EXPECT_NE(&p1, &(node->GetLowKeyPair()));
 	EXPECT_NE(&p2, &(node->GetHighKeyPair()));

 	// Free the node - should not result in an ASAN
 	node->FreeElasticNode();
 	delete bwtree;

}

void InsertElementInNodeRandomTest(){
	auto bwtree = new BPlusTree<int, TupleSlot>;
 	BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
 	BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

 	// Get inner Node
 	auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);
 	
 	std::map<int, int> positions;
 	for(unsigned i = 0; i < 10; i++) {
 		BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
 		p1.first = i;
 		int k;
		k = rand() % (node->GetSize() + 1);
		while(positions.find(k) != positions.end()) k = (k+1) % (node->GetSize()+1);
 		EXPECT_EQ(node->InsertElementIfPossible(p1, node->Begin() + k), true);
 		positions[k] = i;
 		EXPECT_EQ(node->GetSize(), i+1);
 	}

 	using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;

 	unsigned i = 0;
 	for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
 		EXPECT_EQ(element_p->first, positions[i]);
 		i++;
 	}

 	// To Check if we are inserting at the correct place
 	EXPECT_EQ(reinterpret_cast<char *>(node) + 
 		sizeof(BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>>), 
 		reinterpret_cast<char *>(node->Begin()));

 	EXPECT_EQ(&(node->GetLowKeyPair()), node->GetElasticLowKeyPair());
 	EXPECT_EQ(&(node->GetHighKeyPair()), node->GetElasticHighKeyPair());
 	EXPECT_EQ(node->GetLowKeyPair().first, p1.first);
 	EXPECT_EQ(node->GetHighKeyPair().first, p2.first);
 	EXPECT_NE(&p1, &(node->GetLowKeyPair()));
 	EXPECT_NE(&p2, &(node->GetHighKeyPair()));

 	// Free the node - should not result in an ASAN
 	node->FreeElasticNode();
 	delete bwtree;

}

void SplitNodeTest(){
	auto bwtree = new BPlusTree<int, TupleSlot>;
 	BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
 	BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

 	// Get inner Node
 	auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);
 	
 	for(unsigned i = 0; i < 10; i++) {
 		BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
 		p1.first = i;
 		EXPECT_EQ(node->InsertElementIfPossible(p1, node->End()), true);
 		EXPECT_EQ(node->GetSize(), i+1);
 	}

 	auto newnode = node->SplitNode();

 	using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;


 	unsigned i = 0;
 	for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
 		EXPECT_EQ(element_p->first, i);
 		i++;
 	}

 	EXPECT_EQ(i, 5);

 	for (ElementType *element_p = newnode->Begin(); element_p != newnode->End(); element_p++) {
 		EXPECT_EQ(element_p->first, i);
 		i++;
 	}

 	EXPECT_EQ(&(node->GetLowKeyPair()), node->GetElasticLowKeyPair());
 	EXPECT_EQ(&(node->GetHighKeyPair()), node->GetElasticHighKeyPair());
 	EXPECT_EQ(node->GetLowKeyPair().first, p1.first);
 	EXPECT_EQ(node->GetHighKeyPair().first, p2.first);
 	EXPECT_NE(&p1, &(node->GetLowKeyPair()));
 	EXPECT_NE(&p2, &(node->GetHighKeyPair()));

 	EXPECT_EQ(&(newnode->GetLowKeyPair()), newnode->GetElasticLowKeyPair());
 	EXPECT_EQ(&(newnode->GetHighKeyPair()), newnode->GetElasticHighKeyPair());
 	EXPECT_EQ(newnode->GetLowKeyPair().first, p1.first);
 	EXPECT_EQ(newnode->GetHighKeyPair().first, p2.first);
 	EXPECT_NE(&p1, &(newnode->GetLowKeyPair()));
 	EXPECT_NE(&p2, &(newnode->GetHighKeyPair()));

 	// Free the node - should not result in an ASAN
 	node->FreeElasticNode();
 	newnode->FreeElasticNode();
 	delete bwtree;
}

void FindLocationTest(){
	auto bwtree = new BPlusTree<int, TupleSlot>;
 	BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
 	BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

 	// Get inner Node
 	auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);

 	std::set<unsigned> s;
 	while(node->GetSize() < node->GetItemCount()) {
 		int k = rand();
 		while(s.find(k) != s.end()) k++;
 		s.insert(k);
 		BPlusTree<int, TupleSlot>::KeyNodePointerPair p;
 		p.first = k;

 		EXPECT_EQ(node->InsertElementIfPossible(p, node->FindLocation(p, bwtree)),true);
 	}
 	auto iter = node->Begin();
 	for(auto & elem: s) {
 		EXPECT_EQ(iter->first, elem);
 		iter++;
 	}

 	// To Check if we are inserting at the correct place
 	EXPECT_EQ(reinterpret_cast<char *>(node) + 
 		sizeof(BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>>), 
 		reinterpret_cast<char *>(node->Begin()));

 	EXPECT_EQ(&(node->GetLowKeyPair()), node->GetElasticLowKeyPair());
 	EXPECT_EQ(&(node->GetHighKeyPair()), node->GetElasticHighKeyPair());
 	EXPECT_EQ(node->GetLowKeyPair().first, p1.first);
 	EXPECT_EQ(node->GetHighKeyPair().first, p2.first);
 	EXPECT_NE(&p1, &(node->GetLowKeyPair()));
 	EXPECT_NE(&p2, &(node->GetHighKeyPair()));

 	// Free the node - should not result in an ASAN
 	node->FreeElasticNode();
 	delete bwtree;

}


// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, NodeStructuralTests) {

 	BasicNodeInitializationInsertReadAndFreeTest();
 	InsertElementInNodeTest();
 	InsertElementInNodeRandomTest();
 	SplitNodeTest();
 	FindLocationTest();
}
} // namespace terrier::storage::index
