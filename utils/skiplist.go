package utils

import (
	"bytes"
	"github.com/zhangx1n/MyKV/utils/codec"
	"math/rand"
	"sync"
	"time"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
}

func NewSkipList() *SkipList {
	source := rand.NewSource(time.Now().UnixNano())

	return &SkipList{
		header: &Element{
			levels: make([]*Element, defaultMaxLevel),
			Key:    nil,
			Val:    nil,
			score:  0,
		},
		rand:     rand.New(source),
		maxLevel: defaultMaxLevel,
		length:   0,
	}
}

type Element struct {
	levels []*Element
	Key    []byte
	Val    []byte
	score  float64
}

func newElement(score float64, key, val []byte, level int) *Element {
	return &Element{
		levels: make([]*Element, level),
		Key:    key,
		Val:    val,
		score:  score,
	}
}

func (list *SkipList) Add(data *codec.Entry) error {
	list.lock.Lock()
	defer list.lock.Unlock()
	score := list.calcScore(data.Key)
	var elem *Element

	max := len(list.header.levels)
	prevElem := list.header // 记录当前层的前一个元素

	var prevElemHeaders [defaultMaxLevel]*Element // 记录每一层的前一个元素

	for i := max - 1; i >= 0; {
		//keep visit path here
		prevElemHeaders[i] = prevElem
		// 相当于单链表中，cur := head.next; cur!= nil; cur = cur.next
		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
			if comp := list.compare(score, data.Key, next); comp <= 0 {
				if comp == 0 {
					elem = next
					elem.Val = data.Value
					return nil
				}

				//find the insert position
				break
			}

			// 对应单链表 prev = cur;
			prevElem = next
			prevElemHeaders[i] = prevElem
		}

		// topLevel 用来标记在当前层次（i）中，prevElem.levels[i] 的下一个元素。
		// 通过将 topLevel 与前驱元素的下一个元素进行比较，可以决定是否跳过同样的前驱元素，
		// 直接跳到下一个层次，避免不必要的重复查找。
		topLevel := prevElem.levels[i]

		for i--; i >= 0 && prevElem.levels[i] == topLevel; i-- {
			prevElemHeaders[i] = prevElem
		}
	}

	level := list.randLevel()

	elem = newElement(score, data.Key, data.Value, level)

	//to add elem to the skiplist

	for i := 0; i < level; i++ {
		elem.levels[i] = prevElemHeaders[i].levels[i]
		prevElemHeaders[i].levels[i] = elem
	}

	list.length++
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	list.lock.Lock()
	defer list.lock.Unlock()
	if list.length == 0 {
		return nil
	}

	score := list.calcScore(key)

	prevElem := list.header
	i := len(list.header.levels) - 1

	for i >= 0 {
		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
			if comp := list.compare(score, key, next); comp <= 0 {
				if comp == 0 {
					return codec.NewEntry(next.Key, next.Val)
				}
				break
			}

			prevElem = next
		}

		topLevel := prevElem.levels[i]

		for i--; i >= 0 && prevElem.levels[i] == topLevel; i-- {

		}
	}
	return
}

//func (list *SkipList) Remove(key []byte) error {
//	score := list.calcScore(key)
//
//	max := len(list.header.levels)
//	prevElem := list.header
//
//	var prevElemHeaders [defaultMaxLevel]*Element
//	var elem *Element
//
//	for i := max - 1; i >= 0; {
//		//keep visit path here
//		prevElemHeaders[i] = prevElem
//
//		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
//			if comp := list.compare(score, key, next); comp <= 0 {
//				if comp == 0 {
//					elem = next
//				}
//				break
//			}
//
//			//just like linked-list next
//			prevElem = next
//			prevElemHeaders[i] = prevElem
//		}
//
//		topLevel := prevElem.levels[i]
//
//		//to skip same prevHeader's next and fill next elem into temp element
//		for i--; i >= 0 && prevElem.levels[i] == topLevel; i-- {
//			prevElemHeaders[i] = prevElem
//		}
//	}
//
//	if elem == nil {
//		return nil
//	}
//
//	prevTopLevel := len(elem.levels)
//	for i := 0; i < prevTopLevel; i++ {
//		prevElemHeaders[i].levels[i] = elem.levels[i]
//	}
//
//	list.length--
//	return nil
//}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	if score == next.score {
		return bytes.Compare(key, next.Key)
	}

	if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int {
	if list.maxLevel <= 1 {
		return 1
	}
	i := 1
	for ; i < list.maxLevel; i++ {
		if RandN(1000)%2 == 0 {
			return i
		}
	}
	return i
}
