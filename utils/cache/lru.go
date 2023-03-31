package cache

import "container/list"

type windowLRU struct {
	data map[uint64]*list.Element
	cap  int
	list *list.List
}

type storeItem struct {
	stage    int
	key      uint64
	conflict uint64
	value    interface{}
}

func newWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		data: data,
		cap:  size,
		list: list.New(),
	}
}

// 向windowsLru 添加节点 如需要淘汰节点 则需要返回淘汰节点 淘汰的节点会 送往 布隆过滤器
func (lru *windowLRU) add(newitem storeItem) (eitem storeItem, evicted bool) {
	//implement me here!!!
	if len(lru.data) < lru.cap {
		lru.data[newitem.key] = lru.list.PushFront(&newitem)
		return storeItem{}, false
	}

	// windows 需要淘汰

	// 把最后一个节点的值更新 将他放到最前面

	evictedItem := lru.list.Back()
	item := evictedItem.Value.(*storeItem)
	k := item.key
	// 从 data 中将数据删除
	delete(lru.data, k)

	eitem = *item
	*item = newitem
	lru.data[k] = evictedItem
	lru.list.MoveToFront(evictedItem)

	return eitem, true
}

// 对于 WindowsLRU的访问就是 将访问的节点移到首位
func (lru *windowLRU) get(v *list.Element) {
	//implement me here!!!

	lru.list.MoveToFront(v)
	return
}
