# Очередь. Могучая куча

Очередь - добавление в конец и снятие значения по времени добавления. Первый вошел - первый вышел. FIFO

```
Ivan   <-  value
-----      -----
first   -> next   >----
                      |
           value      |
Irina  <-  -----   <<--
           next
                   ....
Igor  <-  value
----      -----    -->  None  
last   ->  next
```

```py

class Node:
    def __init__(self, val):
        self.value = val
        self.next = None


# Реализация очереди при помощи связанного списка
class Queue:
    def __init__(self):
        self.first = None
        self.last = None
    
    def is_empty(self):
        return self.first is None
    
    def enqueue(self, val):
        if self.is_empty():
            # если очередь пуста, первый добавленный элемент и есть последний
            self.first = self.last = Node(val)
        else:
            self.last.next = Node(val)
            self.last = self.last.next
    
    def dequeue(self):
        if self.is_empty():
            raise RunTimeError("Queue is empty")
        val = self.first.value
        self.first = self.first.next
        return val 
```

Приоритизированная очередь - 1й войдет тот, у кого приоритет выше.

