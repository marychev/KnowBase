# Heapsort and Binary Heap. Пирамидальная сортировка и Двоичная куча

Это простейший вариант кучи.

Пример двоичной кучи с частичным убыванием.

```
15(l1)
13(l2) 14(l3)
9(l4)  11(l5) 12(l6) 14(l7)
8(l8)  2(l9)  1(l10) 10(l11) 8(l12) 6(l13) 9(l14) 7(l15)
...
```

Для N=8 необходимо 4 уровня: `log2(8) = 3`. 
Для N=7 необходимо 3 уровня: `log2(7) = 2.78`.
8 <= N <= 15 - 4 уровня, `log2(15) = 3.91`

Количество слоев `L` для хранения `N` элементовт в двоичной куче = 
```
           # округлить
L(N) = 1 + log2(N)   
```

#### Сложности алгоритмов (Big O)

- добавление в приоритезированную очередь - `O(lon(N))`
- снятие с начала очереди элемента с наивысшим приоритетом -  `O(lon(N))`
- подсчет кол-ва эл-в в очереди  `O(1)`


### Пример 

```py
# Двоичная куча с частичным убыванием
class Enrty:
    def __init__(self, v, p):
        self.value = v
        self.priority = p
    
class PQ:
    def __init__(self, size):
        self.size = size
        self.storage = [None] * (size + 1)
        self.N = 0
    
    def less(self, i, j):
        # поверяет что storage[i] имеет меньший приоритет 
        # чем storage[j]
    
    def __swap__(self, i, j):
        # меняет местами j-й и i-й узел
        self.storage[i], self.storage[j] = self.storage[j], self.storage[i]

    def enqueue(self, v, p):
        self.N = self.size:
            raise RuntimeError("Priority queue is full")
        # Чтобы добавить эл-т в кучу нужно разместить его в 1й свободной
        # ячейке массива, а затем всплыть
        self.N += 1
        self.storage = [self.N] = Entry(v, p)
        self.swim(self.N)

    def swim(self, child):
        # перестроить storage, возвращая куче пирамидальность!
        while child > 1 and self.less(child//2, child):
            # родительский узел находится в storage[child//2]
            self.swap(child, child//2):
            child = child // 2
        ...
```
