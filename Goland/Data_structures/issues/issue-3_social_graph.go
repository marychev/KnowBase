// Реализуйте систему для хранения пользователей и связей между ними (социальный граф).

package main

import (
    "fmt"
    "sync"
)

type User struct {
    ID   int
    Name string
}

type Graph struct {
	mu          sync.RWMutex
	users	  	map[int]*User
	connections map[int]map[int]bool // connections[fromID][toID] = true
	typed       map[int]map[int]Connection // typed[fromID][toID] = Connection
}

func NewGraph() *Graph {
    return &Graph{
        users:       make(map[int]*User),
        connections: make(map[int]map[int]bool),
        typed:         make(map[int]map[int]Connection),
    }
}

func (g *Graph) AddUser(id int, name string) {
    g.mu.Lock()
    defer g.mu.Unlock()
    g.users[id] = &User{ID: id, Name: name}
}

func (g *Graph) GetUser(id int) (*User, bool) {
    g.mu.RLock()
    defer g.mu.RUnlock()
    user, ok := g.users[id]
    return user, ok
}

func (g *Graph) AddConnection(fromID, toID int) bool {
    g.mu.Lock()
    defer g.mu.Unlock()

    if _, ok := g.users[fromID]; !ok {
        return false
    }
    if _, ok := g.users[toID]; !ok {
        return false
    }
    if g.connections[fromID] == nil {
        g.connections[fromID] = make(map[int]bool)
    }
    g.connections[fromID][toID] = true
    return true
}

func (g *Graph) GetConnections(userID int) []*User {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var result []*User
	for toID := range g.connections[userID] {
		if user, ok := g.users[toID]; ok {
			result = append(result, user)
		}
	}
	return result
}

// hasConnectionLocked — версия без блокировки. Вызывающий обязан держать g.mu.
// Существует, чтобы IsMutual мог сделать две проверки под одним RLock, не устраивая
// рекурсивный RLock (он у RWMutex запрещён).
func (g *Graph) hasConnectionLocked(fromID, toID int) bool {
    return g.connections[fromID][toID]
}

func (g *Graph) HasConnection(fromID, toID int) bool {
    g.mu.RLock()
    defer g.mu.RUnlock()
    return g.hasConnectionLocked(fromID, toID)
}

func (g *Graph) UserCount() int {
    g.mu.RLock()
    defer g.mu.RUnlock()
    return len(g.users)
}

func (g *Graph) RemoveConnection(fromID, toID int) bool {
    g.mu.Lock()
    defer g.mu.Unlock()

    had := g.connections[fromID][toID]
	delete(g.connections[fromID], toID)
	_, hadTyped := g.typed[fromID][toID]
	delete(g.typed[fromID], toID)
    return had || hadTyped
}

func (g *Graph) RemoveUser(id int) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.users[id]; !ok {
		return false
	}

	delete(g.users, id)
	
	delete(g.connections, id)
	for _, targets := range g.connections {
		delete(targets, id)
	}

	delete(g.typed, id)
	for _, targets := range g.typed {
		delete(targets, id)
	}
	return true
}

func (g *Graph) IsMutual(id1, id2 int) bool {
    g.mu.RLock()
    defer g.mu.RUnlock()
    return g.hasConnectionLocked(id1, id2) && g.hasConnectionLocked(id2, id1)
}

func (g *Graph) ConnectionCount(userID int) int {
    g.mu.RLock()
    defer g.mu.RUnlock()
    return len(g.connections[userID])
}

func (g *Graph) CommonConnections(id1, id2 int) []*User {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var result []*User
	for toID := range g.connections[id1] {
		if g.connections[id2][toID] {
			if user, ok := g.users[toID]; ok {
				result = append(result, user)
			}
		}
	}
	return result
}

func (g *Graph) SuggestConnections(userID int) []*User {
	g.mu.RLock()
	defer g.mu.RUnlock()

	direct := g.connections[userID]   // прямые друзья (nil-safe)
	suggestions := make(map[int]bool) // set рекомендаций

	for friendID := range direct {
		for foafID := range g.connections[friendID] { 
			if foafID == userID { continue } // не предлагать самого себя
			if direct[foafID] { continue } // не предлагать уже существующих друзей
			suggestions[foafID] = true	
		}
	}
	// Превратить set в []*User
	var result []*User
	for id := range suggestions {
		if user, ok := g.users[id]; ok {
			result = append(result, user)
		}
	}
	return result
}

func (g *Graph) GetAllUsers() []*User {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var result []*User
	for _, user := range g.users {
		result = append(result, user)
	}
	return result
}


type Connection interface {
    Type() string
    Weight() int
}

type Friend struct {
    Since string // Дата начала дружбы
}

func (f Friend) Type() string {
    return "friend"
}

func (f Friend) Weight() int {
    return 2
}


type Follower struct {
    Notifications bool
}

func (f Follower) Type() string {
    return "follower"
}

func (f Follower) Weight() int {
    return 1
}

type Blocked struct {
    Reason string
}

func (b Blocked) Type() string {
    return "blocked"
}

func (b Blocked) Weight() int {
    return -1
}

func (g *Graph) AddTypedConnection(fromID, toID int, conn Connection) bool {
    g.mu.Lock()
    defer g.mu.Unlock()

    if _, ok := g.users[fromID]; !ok { return false }
	if _, ok := g.users[toID]; !ok { return false }
	if g.typed[fromID] == nil {
		g.typed[fromID] = make(map[int]Connection)
	}
	g.typed[fromID][toID] = conn
    return true
}

func (g *Graph) GetConnectionsByType(userID int, connType string) []*User {
    g.mu.RLock()
    defer g.mu.RUnlock()

    var result []*User
	for toID, conn := range g.typed[userID] {
		if conn.Type() == connType {
			if user, ok := g.users[toID]; ok {
				result = append(result, user)
			}
		}
	}
    return result
}

func (g *Graph) GetConnectionInfo(fromID, toID int) (Connection, bool) {
    g.mu.RLock()
    defer g.mu.RUnlock()
    conn, ok := g.typed[fromID][toID]
	return conn, ok
}


func main() {
    graph := NewGraph()

    graph.AddUser(1, "Alice")
    graph.AddUser(2, "Bob")
    graph.AddUser(3, "Charlie")

    graph.AddConnection(1, 2) // Alice -> Bob
    graph.AddConnection(1, 3) // Alice -> Charlie
    graph.AddConnection(2, 3) // Bob -> Charlie

    if user, ok := graph.GetUser(1); ok {
        fmt.Printf("User: %s\n", user.Name)
        friends := graph.GetConnections(1)
        fmt.Printf("Friends: %d\n", len(friends))
        for _, friend := range friends {
            fmt.Printf("  - %s\n", friend.Name)
        }
    }

    fmt.Printf("Alice and Bob connected: %v\n", 
        graph.HasConnection(1, 2))

	graph.RemoveConnection(1, 2)
	fmt.Println(graph.HasConnection(1, 2))     // false
	fmt.Println(graph.ConnectionCount(1))       // 1 (только Charlie остался)

	graph.AddConnection(2, 1)                    // взаимная связь
	fmt.Println(graph.IsMutual(1, 3))           // false
	graph.AddConnection(3, 1)
	fmt.Println(graph.IsMutual(1, 3))           // true

	graph.RemoveUser(3)
	fmt.Println(graph.UserCount())              // 2
	fmt.Println(graph.HasConnection(2, 3))      // false — Bob→Charlie ушёл
	fmt.Println(graph.HasConnection(1, 3))      // false — и обратная Alice→Charlie тоже (RemoveConnection убрал раньше)


	// Пример проверки
	common := graph.CommonConnections(1, 2)  // общие друзья Alice и Bob
	fmt.Println("common:", len(common))

	suggestions := graph.SuggestConnections(1)  // "друзей друзей" для Alice
	fmt.Println("suggestions:", len(suggestions))

	all := graph.GetAllUsers()
	fmt.Println("total users:", len(all))

	fmt.Println("--- friends tests ---")
	g2 := NewGraph()
	g2.AddUser(1, "Alice")
	g2.AddUser(2, "Bob")
	g2.AddUser(3, "Charlie")
	g2.AddUser(4, "Dan")
	g2.AddConnection(1, 2)
	g2.AddConnection(1, 3)
	g2.AddConnection(2, 3)
	g2.AddConnection(2, 4)
	fmt.Println(len(g2.CommonConnections(1, 2)))  // 1
	fmt.Println(len(g2.SuggestConnections(1)))    // 1 (Dan)
	fmt.Println(len(g2.GetAllUsers()))            // 4


	g3 := NewGraph()
	g3.AddUser(1, "Alice")
	g3.AddUser(2, "Bob")
	g3.AddUser(3, "Charlie")

	g3.AddTypedConnection(1, 2, Friend{})
	g3.AddTypedConnection(1, 3, Follower{})
	g3.AddTypedConnection(2, 3, Blocked{})

	friends := g3.GetConnectionsByType(1, "friend")
	fmt.Println("friends:", len(friends))                // 1 (Bob)

	if info, ok := g3.GetConnectionInfo(2, 3); ok {
		fmt.Printf("2→3: type=%s weight=%d\n", info.Type(), info.Weight())
		// 2→3: type=blocked weight=-1
	}

	// go run -race issues/issue-3_social_graph.go
	gr := NewGraph()
	for i := 0; i < 20; i++ {
		gr.AddUser(i, fmt.Sprintf("u%d", i))
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		from, to := i%20, (i+1)%20

		wg.Add(1)
		go func() {
			defer wg.Done()
			gr.AddConnection(from, to)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			gr.HasConnection(from, to)
			gr.IsMutual(from, to)
			_ = gr.GetConnections(from)
			_ = gr.UserCount()
		}()
	}
	wg.Wait()
	fmt.Println("race stress done, users:", gr.UserCount())
}