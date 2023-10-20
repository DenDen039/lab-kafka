package main

import (
	"container/heap"
)

type TransactionsHeap []*Transaction

func (h TransactionsHeap) Len() int           { return len(h) }
func (h TransactionsHeap) Less(i, j int) bool { return h[i].Data.Price < h[j].Data.Price }
func (h TransactionsHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *TransactionsHeap) Push(x interface{}) {
	*h = append(*h, x.(*Transaction))
}

func (h *TransactionsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *TransactionsHeap) AddElement(transaction *Transaction) bool {
	if h.Len() < 10 {
		heap.Push(h, transaction)
		return true
	}
	if transaction.Data.Price > (*h)[0].Data.Price {
		heap.Pop(h)
		heap.Push(h, transaction)
		return true
	}
	return false
}

func getHeapOrder(h *TransactionsHeap) []*Transaction {
	tempHeap := make(TransactionsHeap, len(*h))
	copy(tempHeap, *h)

	elements := make([]*Transaction, 0)
	for tempHeap.Len() != 0 {
		elements = append([]*Transaction{heap.Pop(&tempHeap).(*Transaction)}, elements...)
	}

	return elements
}
