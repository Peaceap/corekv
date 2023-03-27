package utils

import (
	"bytes"
	"fmt"
	"github.com/hardcore-os/corekv/utils/codec"
	"math/rand"
	"sync"
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
	size     int64
}

func NewSkipList() *SkipList {
	//初始化头节点
	//头尾节点应该有最高层数
	header := &Element{
		levels: make([]*Element, defaultMaxLevel),
	}

	return &SkipList{
		header:   header,
		size:     0,
		maxLevel: defaultMaxLevel - 1,
		rand:     r,
	}
}

type Element struct {
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level+1),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	list.lock.Lock()
	defer list.lock.Unlock()

	prevs := make([]*Element, defaultMaxLevel)

	key := data.Key
	keyScore := list.calcScore(key)
	header, maxLevel := list.header, list.maxLevel
	prev := header
	for i := maxLevel; i >= 0; i-- {
		//初始化头节点
		//最开始肯定是继承上一层节点传下来的
		prevs[i] = prev
		for ne := prev.levels[i]; ne != nil; ne = prev.levels[i] {
			//fmt.Println("层数",i)
			//比较 返回-1是 目标score 小于 ne的score 也就是找到了第一个比要插入节点的值大的位置
			if comp := list.compare(keyScore, key, ne); comp <= 0 {
				if comp == 0 {
					// 更新数据
					ne.entry = data
					//相当于出错了
					return nil
				}
				// 如果走到这 相当于 找到了第一个比要插入节点的值大的位置 此时因为直接break
				// prevs[i] 没有更新 所以 prevs[i]里面存的就是 i层 目标节点的前一个节点
				// prev 就是 目标节点的前一个节点 进入下一层 去找有没有更接近目标节点的节点 一步一步向它靠近
				break

			}
			prev = ne
			prevs[i] = prev
		}

	}

	randLevel, keyScore := list.randLevel(), list.calcScore(key)
	e := newElement(keyScore, data, randLevel)

	for i := randLevel; i >= 0; i-- {
		if prevs[i] == nil {
			fmt.Println("????????")
		}
		e.levels[i] = prevs[i].levels[i]
		prevs[i].levels[i] = e
	}
	return nil

}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {

	list.lock.Lock()
	list.lock.Unlock()
	score := list.calcScore(key)
	pre := list.header
	level := list.maxLevel - 1

	for i := level; i >= 0; i-- {

		for l := pre.levels[i]; l != nil; l = pre.levels[i] {
			//compare（A.score,A,B）
			//比较 返回-1是 A 小于 B 也就是找到了第一个比要插入节点的值大的位置

			if ret := list.compare(score, key, l); ret <= 0 {
				if ret == 0 {
					return l.entry
				}
				break
			}
			//update pre
			pre = l

		}
	}

	return nil
}

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
	//implement me here!!!

	if next == nil {
		return 1
	}

	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}
	if score < next.score {
		return -1
	} else {
		return 1
	}

}

func (list *SkipList) randLevel() int {
	//implement me here!!!
	for i := 0; i < list.maxLevel; i++ {
		if list.rand.Intn(2) == 0 {
			return i
		}
	}

	return list.maxLevel
}

func (list *SkipList) Size() int64 {
	//implement me here!!!
	return list.size
}
