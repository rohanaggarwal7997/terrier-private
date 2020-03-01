#include "test_util/test_harness.h"
#include "storage/index/bplustree.h"
#include "storage/storage_defs.h"

#include <stdlib.h> 
#include <unordered_map>
#include <set>
#include <unordered_set>

namespace terrier::storage::index {

struct BPlusTreeTests : public TerrierTest {};

void BasicNodeInitializationInsertReadAndFreeTest(){
  auto bplustree = new BPlusTree<int, TupleSlot>;
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
  delete bplustree;

}

void InsertElementInNodeTest(){
  auto bplustree = new BPlusTree<int, TupleSlot>;
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
  delete bplustree;

}

void InsertElementInNodeRandomTest(){
  auto bplustree = new BPlusTree<int, TupleSlot>;
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
  delete bplustree;

}

void SplitNodeTest(){
  auto bplustree = new BPlusTree<int, TupleSlot>;
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

  EXPECT_EQ(i, 10);

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
  delete bplustree;
}

void FindLocationTest(){
  auto bplustree = new BPlusTree<int, TupleSlot>;
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
    EXPECT_EQ(node->InsertElementIfPossible(p, node->FindLocation(k, bplustree)),true);
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
  delete bplustree;

}

void PopBeginTest() {

  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);
  
  // To check if we can read what we inserted
  std::vector<BPlusTree<int, TupleSlot>::KeyNodePointerPair> values;
  for(unsigned i = 0; i < 10; i++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    p1.first = i;
    node->PushBack(p1);
    EXPECT_EQ(node->GetSize(), i+1);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;
  unsigned i = 0;
  while(node->PopBegin()) {
  	i++;
  	unsigned j = i;
	for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
	  EXPECT_EQ(element_p->first, j);
	  j++;
	}
	EXPECT_EQ(j, 10);
  }

  EXPECT_EQ(i, 10);

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
  delete bplustree;


}


// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, NodeStructuralTests) {

  BasicNodeInitializationInsertReadAndFreeTest();
  InsertElementInNodeTest();
  InsertElementInNodeRandomTest();
  SplitNodeTest();
  FindLocationTest();
  PopBeginTest();
}

void BasicBPlusTreeInsertTestNoSplittingOfRoot() {

  auto bplustree = new BPlusTree<int, TupleSlot>;
  for(unsigned i=0; i<100; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = i;
    bplustree->Insert(p1);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyElementPair;

  auto node = reinterpret_cast<BPlusTree<int, TupleSlot>::ElasticNode<ElementType> *>(bplustree->GetRoot()); 
  unsigned i = 0;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, i);
    i++;
  }
  EXPECT_EQ(i, 100);
  bplustree->FreeTree();
  delete bplustree;
}

void BasicBPlusTreeInsertTestSplittingOfRootOnce() {

  auto bplustree = new BPlusTree<int, TupleSlot>;
  for(unsigned i=0; i<129; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = i;
    bplustree->Insert(p1);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyElementPair;
  using KeyPointerType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;

  auto node = reinterpret_cast<BPlusTree<int, TupleSlot>::ElasticNode<ElementType> *>
    (bplustree->GetRoot()->GetLowKeyPair().second);
  auto noderoot = reinterpret_cast<BPlusTree<int, TupleSlot>::ElasticNode<KeyPointerType> *>
    (bplustree->GetRoot());
  auto node2 =  reinterpret_cast<BPlusTree<int, TupleSlot>::ElasticNode<ElementType> *>
    (noderoot->Begin()->second);
  unsigned i = 0;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, i);
    i++;
  }
  EXPECT_EQ(i, 64);
  for (ElementType *element_p = node2->Begin(); element_p != node2->End(); element_p++) {
    EXPECT_EQ(element_p->first, i);
    i++;
  }
  EXPECT_EQ(i, 129);

  // Count no of elements in root node - should be 1
  i = 0;
  for (KeyPointerType * element_p = noderoot->Begin(); element_p!=noderoot->End(); element_p ++)
  	i++;

  EXPECT_EQ(i, 1);

  // Only freeing these should free us of any ASAN
  bplustree->FreeTree();
  delete bplustree;
}

void LargeKeySequentialInsertAndRetrievalTest() {

  auto bplustree = new BPlusTree<int, TupleSlot>;
  for(unsigned i=0; i<100000; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = i;
    bplustree->Insert(p1);
  }

  for(int i=0; i<100000; i++) {
  	EXPECT_EQ(bplustree->IsPresent(i), true);
  }

  for(int i = 100000; i < 200000; i++) {
  	EXPECT_EQ(bplustree->IsPresent(i), false);
  }

  bplustree->FreeTree();
  delete bplustree;
}

void LargeKeyRandomInsertAndRetrievalTest() {

  auto bplustree = new BPlusTree<int, TupleSlot>;
  bplustree->SetInnerNodeSizeUpperThreshold(5);
  bplustree->SetLeafNodeSizeUpperThreshold(5);
  std::set<int> keys;
  for(unsigned i=0; i<100000; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand()%500000;
    while(keys.find(k) != keys.end()) k++;
    keys.insert(k); 
    p1.first = k;
    bplustree->Insert(p1);
  }

  for(int i=0; i<500000; i++) {
    if(keys.find(i) != keys.end()) {
      EXPECT_EQ(bplustree->IsPresent(i), true);
    } else {
      EXPECT_EQ(bplustree->IsPresent(i), false);
    }
  }
  // hardcoded - maybe wrong
  EXPECT_EQ(bplustree->GetRoot()->GetDepth(), 7);

  bplustree->FreeTree();
  delete bplustree;
}

void StructuralIntegrityTestWithRandomInsert() {

  auto bplustree = new BPlusTree<int, TupleSlot>;
  // The size is set to 2 more because of the following
  // When we split an inner node, we might end up deleting an element
  // from right side without putting anything in the right side
  // Hence the size may be 31 at some points if we used 64. 
  bplustree->SetInnerNodeSizeUpperThreshold(66);
  bplustree->SetLeafNodeSizeUpperThreshold(66);
  bplustree->SetInnerNodeSizeLowerThreshold(32);
  bplustree->SetLeafNodeSizeLowerThreshold(32);
  std::set<int> keys;
  for(unsigned i=0; i<100000; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand()%500000;
    while(keys.find(k) != keys.end()) k++;
    keys.insert(k); 
    p1.first = k;
    bplustree->Insert(p1);
  }

  EXPECT_EQ(bplustree->StructuralIntegrityVerification(*keys.begin(), *keys.rbegin(),
    keys, bplustree->GetRoot()), true);
  // All keys found in the tree
  EXPECT_EQ(keys.size(), 0);

  bplustree->FreeTree();
  delete bplustree;  
}

void LargeKeyRandomInsertSiblingSequenceTest() {

  // Insert keys
  auto bplustree = new BPlusTree<int, TupleSlot>;
  bplustree->SetInnerNodeSizeUpperThreshold(5);
  bplustree->SetLeafNodeSizeUpperThreshold(5);
  std::set<int> keys;
  for(unsigned i=0; i<100000; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand()%500000;
    while(keys.find(k) != keys.end()) k++;
    keys.insert(k);
    p1.first = k;
    bplustree->Insert(p1);
  }

  EXPECT_EQ(bplustree->SiblingForwardCheck(keys), true);
  EXPECT_EQ(bplustree->SiblingBackwardCheck(keys), true);
  bplustree->FreeTree();
  delete bplustree;
}

void DuplicateKeyValueInsertTest() {

  // Insert Keys
  auto bplustree = new BPlusTree<int, int>;
  bplustree->SetInnerNodeSizeUpperThreshold(5);
  bplustree->SetLeafNodeSizeUpperThreshold(5);
  std::unordered_map<int, std::vector<int> > keys_values;
  for(unsigned i=0; i<100000; i++) {
    int k = rand()%1000;
    int v = rand()%500000;
    if(keys_values.count(k) == 0) {
      std::vector<int> value_list;
      value_list.push_back(v);
      keys_values[k] = value_list;
    }
    else {
      keys_values[k].push_back(v);
    }
    BPlusTree<int, int>::KeyElementPair p1;
    p1.first = k;
    p1.second = v;
    bplustree->Insert(p1);
  }
  EXPECT_EQ(bplustree->DuplicateKeyValuesCheck(keys_values), true);
  bplustree->FreeTree();
  delete bplustree;
}
void ScanKeyTest() {

  // Insert Keys
  auto bplustree = new BPlusTree<int, int>;
  bplustree->SetInnerNodeSizeUpperThreshold(5);
  bplustree->SetLeafNodeSizeUpperThreshold(5);
  std::unordered_map<int, std::vector<int> > keys_values;
  for(unsigned i=0; i<100000; i++) {
    int k = rand()%1000;
    int v = rand()%500000;
    if(keys_values.count(k) == 0) {
      std::vector<int> value_list;
      value_list.push_back(v);
      keys_values[k] = value_list;
    }
    else {
      keys_values[k].push_back(v);
    }
    bplustree->Insert(BPlusTree<int, int>::KeyElementPair(k, v));
  }
  auto itr_map = keys_values.begin();
  while(itr_map != keys_values.end()) {
    int k = itr_map->first;
    std::vector<int> values = keys_values[k];
    std::vector<int> result;
    bplustree->FindValueOfKey(k, result);
    for(unsigned i = 0; i < values.size(); i++) {
      EXPECT_EQ(values[i] == result[i], true);
    }
    itr_map++;
  }
  bplustree->FreeTree();
  delete bplustree;
}


void LargeStructuralIntegrityVerificationTest() {

  auto bplustree = new BPlusTree<int, int>;
  // The size is set to 2 more because of the following
  // When we split an inner node, we might end up deleting an element
  // from right side without putting anything in the right side
  // Hence the size may be 31 at some points if we used 64. 
  bplustree->SetInnerNodeSizeUpperThreshold(8);
  bplustree->SetLeafNodeSizeUpperThreshold(8);
  bplustree->SetInnerNodeSizeLowerThreshold(3);
  bplustree->SetLeafNodeSizeLowerThreshold(3);
  std::set<int> keys;
  for(unsigned i=0; i<1000; i++) {
    BPlusTree<int, int>::KeyElementPair p1;
    int k = rand()%5000;
    while(keys.find(k) != keys.end()) k++;
    keys.insert(k); 
    p1.first = k;
    p1.second = 1;
    bplustree->Insert(p1);

    auto keys_copy = keys;

    // std::cout<<"Inserted "<<p1.first<<std::endl;
    // bplustree->PrintTree();

    // Structural Integrity Verification Everytime
    EXPECT_EQ(bplustree->StructuralIntegrityVerification(*keys_copy.begin(), *keys_copy.rbegin(),
    keys_copy, bplustree->GetRoot()), true);
    EXPECT_EQ(keys_copy.size(), 0);
  }

  // Free Everything
  bplustree->FreeTree();
  delete bplustree;  
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, InsertTests) {

  BasicBPlusTreeInsertTestNoSplittingOfRoot();
  BasicBPlusTreeInsertTestSplittingOfRootOnce();
  LargeKeySequentialInsertAndRetrievalTest();
  LargeKeyRandomInsertAndRetrievalTest();
  StructuralIntegrityTestWithRandomInsert();
  LargeKeyRandomInsertSiblingSequenceTest();
  DuplicateKeyValueInsertTest();
  ScanKeyTest();
  LargeStructuralIntegrityVerificationTest();
  // LargeKeyInsertUniqueAndRetrievalTest();
}

} // namespace terrier::storage::index
