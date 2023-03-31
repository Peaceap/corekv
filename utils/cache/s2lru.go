package cache

import "container/list"

type segmentedLRU struct {
	data                     map[uint64]*list.Element
	stageOneCap, stageTwoCap int
	stageOne, stageTwo       *list.List
}

const (
	STAGE_ONE = iota
	STAGE_TWO
)

func newSLRU(data map[uint64]*list.Element, stageOneCap, stageTwoCap int) *segmentedLRU {
	return &segmentedLRU{
		data:        data,
		stageOneCap: stageOneCap,
		stageTwoCap: stageTwoCap,
		stageOne:    list.New(),
		stageTwo:    list.New(),
	}
}

// 每一个新添加的数据都会把它放在 缓刑区
func (slru *segmentedLRU) add(newitem storeItem) {
	//implement me here!!!
	newitem.stage = STAGE_ONE

	// 如果 缓刑区还没满
	if slru.stageOne.Len() < slru.stageOneCap || slru.Len() < slru.stageOneCap+slru.stageTwoCap {

		slru.data[newitem.key] = slru.stageOne.PushFront(&newitem)

		return
	}

	// 缓刑区 需要淘汰

	evictedItem := slru.stageOne.Back()
	item := evictedItem.Value.(*storeItem)
	k := item.key
	// 从 data 中将数据删除
	delete(slru.data, k)

	*item = newitem
	slru.data[k] = evictedItem
	slru.stageOne.MoveToFront(evictedItem)
	return
}

// 调用get 就说明已经至少第二次访问 应该将其放入保护区
func (slru *segmentedLRU) get(v *list.Element) {
	//implement me here!!!

	item := v.Value.(*storeItem)
	//已经在保护区
	if item.stage == STAGE_TWO {
		slru.stageTwo.MoveToFront(v)
		return
	}

	// 如果保护区没满
	if slru.stageTwo.Len() < slru.stageOneCap {
		ne := slru.stageTwo.PushFront(v)
		item.stage = STAGE_TWO
		k := item.key
		slru.data[k] = ne
		return
	}

	// 如果保护区满了 要给保护区最后一个 放回缓刑区的第一个

	stageTwoback := slru.stageTwo.Back()
	bitem := stageTwoback.Value.(*storeItem)

	//将要插入的节点 先放到保护区的最后一个
	*item, *bitem = *bitem, *item

	//item 现在是从缓冲区 取下来的节点 应该放到缓刑区的第一个节点
	item.stage = STAGE_ONE
	slru.data[item.key] = v
	slru.stageOne.PushFront(v)

	//保护区的最后一个节点应该放到 stageTwo的第一个节点
	bitem.stage = STAGE_TWO
	slru.data[bitem.key] = stageTwoback
	slru.stageTwo.PushFront(stageTwoback)
}

func (slru *segmentedLRU) Len() int {
	return slru.stageTwo.Len() + slru.stageOne.Len()
}

func (slru *segmentedLRU) victim() *storeItem {
	if slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		return nil
	}

	v := slru.stageOne.Back()
	return v.Value.(*storeItem)
}
