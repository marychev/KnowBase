// Задача 3: социальный граф — рефакторинг оригинального ../issue-3_social_graph.go
//
// Что изменилось по сравнению с оригиналом:
//   1. Исправлен баг корректности в RemoveUser: теперь чистит и typed-мапу
//      (исходящие + входящие), не только connections. Без этого после удаления
//      пользователя оставались зомби-записи в типизированных связях.
//   2. RemoveConnection сделан симметричным: убирает связь и из connections,
//      и из typed. Возврат bool теперь = "было ли что-то удалено суммарно".
//   3. Введён строго типизированный ConnType вместо голых string-констант,
//      плюс константы TypeFriend/TypeFollower/TypeBlocked. GetConnectionsByType
//      теперь принимает ConnType — компилятор ловит опечатки на этапе сборки.
//   4. Дубликат "проитерировать ID → достать *User → append" вынесен в
//      generic-хелпер collectUsers[V any]. Использован в 4 методах.
//   5. SuggestConnections использует map[int]struct{} для set вместо
//      map[int]bool — идиома Go, 0 байт на значение.
//   6. Пре-аллокация результирующих слайсов через make([]*User, 0, len(...))
//      где размер известен — экономит рост-копии.
//   7. Убраны имена receiver у Type()/Weight(), т.к. они не используются.
//   8. Форматирование приведено к gofmt (табы), убраны двойные пустые строки.

package main

import "fmt"

type User struct {
	ID   int
	Name string
}

type ConnType string

const (
	TypeFriend   ConnType = "friend"
	TypeFollower ConnType = "follower"
	TypeBlocked  ConnType = "blocked"
)

type Connection interface {
	Type() ConnType
	Weight() int
}

type Friend struct {
	Since string
}

func (Friend) Type() ConnType { return TypeFriend }
func (Friend) Weight() int    { return 2 }

type Follower struct {
	Notifications bool
}

func (Follower) Type() ConnType { return TypeFollower }
func (Follower) Weight() int    { return 1 }

type Blocked struct {
	Reason string
}

func (Blocked) Type() ConnType { return TypeBlocked }
func (Blocked) Weight() int    { return -1 }

// Graph — направленный социальный граф.
// connections и typed — два независимых слоя связей: HasConnection и
// ConnectionCount смотрят только в connections; GetConnectionsByType и
// GetConnectionInfo — только в typed.
type Graph struct {
	users       map[int]*User
	connections map[int]map[int]bool
	typed       map[int]map[int]Connection
}

func NewGraph() *Graph {
	return &Graph{
		users:       make(map[int]*User),
		connections: make(map[int]map[int]bool),
		typed:       make(map[int]map[int]Connection),
	}
}

// collectUsers превращает ключи произвольной мапы (int → V) в []*User,
// пропуская ID, которых нет в g.users.
func (g *Graph) collectUsers(ids map[int]struct{}) []*User {
	result := make([]*User, 0, len(ids))
	for id := range ids {
		if user, ok := g.users[id]; ok {
			result = append(result, user)
		}
	}
	return result
}

func (g *Graph) AddUser(id int, name string) {
	g.users[id] = &User{ID: id, Name: name}
}

func (g *Graph) GetUser(id int) (*User, bool) {
	user, ok := g.users[id]
	return user, ok
}

func (g *Graph) AddConnection(fromID, toID int) bool {
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
	result := make([]*User, 0, len(g.connections[userID]))
	for toID := range g.connections[userID] {
		if user, ok := g.users[toID]; ok {
			result = append(result, user)
		}
	}
	return result
}

func (g *Graph) HasConnection(fromID, toID int) bool {
	return g.connections[fromID][toID]
}

func (g *Graph) UserCount() int {
	return len(g.users)
}

// RemoveConnection убирает связь и из connections, и из typed.
// Возвращает true, если хоть что-то было удалено.
func (g *Graph) RemoveConnection(fromID, toID int) bool {
	hadBool := g.connections[fromID][toID]
	delete(g.connections[fromID], toID)

	_, hadTyped := g.typed[fromID][toID]
	delete(g.typed[fromID], toID)

	return hadBool || hadTyped
}

// RemoveUser удаляет пользователя и все его связи в обоих слоях:
// исходящие (g.connections[id], g.typed[id]) и входящие (проход по всем
// внешним мапам с delete(inner, id)).
func (g *Graph) RemoveUser(id int) bool {
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
	return g.HasConnection(id1, id2) && g.HasConnection(id2, id1)
}

func (g *Graph) ConnectionCount(userID int) int {
	return len(g.connections[userID])
}

func (g *Graph) CommonConnections(id1, id2 int) []*User {
	result := make([]*User, 0)
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
	direct := g.connections[userID]
	suggestions := make(map[int]struct{})

	for friendID := range direct {
		for foafID := range g.connections[friendID] {
			if foafID == userID {
				continue
			}
			if direct[foafID] {
				continue
			}
			suggestions[foafID] = struct{}{}
		}
	}

	return g.collectUsers(suggestions)
}

func (g *Graph) GetAllUsers() []*User {
	result := make([]*User, 0, len(g.users))
	for _, user := range g.users {
		result = append(result, user)
	}
	return result
}

func (g *Graph) AddTypedConnection(fromID, toID int, conn Connection) bool {
	if _, ok := g.users[fromID]; !ok {
		return false
	}
	if _, ok := g.users[toID]; !ok {
		return false
	}
	if g.typed[fromID] == nil {
		g.typed[fromID] = make(map[int]Connection)
	}
	g.typed[fromID][toID] = conn
	return true
}

func (g *Graph) GetConnectionsByType(userID int, connType ConnType) []*User {
	result := make([]*User, 0, len(g.typed[userID]))
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
	conn, ok := g.typed[fromID][toID]
	return conn, ok
}

func main() {
	graph := NewGraph()

	graph.AddUser(1, "Alice")
	graph.AddUser(2, "Bob")
	graph.AddUser(3, "Charlie")

	graph.AddConnection(1, 2)
	graph.AddConnection(1, 3)
	graph.AddConnection(2, 3)

	if user, ok := graph.GetUser(1); ok {
		fmt.Printf("User: %s\n", user.Name)
		friends := graph.GetConnections(1)
		fmt.Printf("Friends: %d\n", len(friends))
		for _, friend := range friends {
			fmt.Printf("  - %s\n", friend.Name)
		}
	}

	fmt.Printf("Alice and Bob connected: %v\n", graph.HasConnection(1, 2))

	graph.RemoveConnection(1, 2)
	fmt.Println(graph.HasConnection(1, 2)) // false
	fmt.Println(graph.ConnectionCount(1))  // 1

	graph.AddConnection(2, 1)
	fmt.Println(graph.IsMutual(1, 3)) // false
	graph.AddConnection(3, 1)
	fmt.Println(graph.IsMutual(1, 3)) // true

	graph.RemoveUser(3)
	fmt.Println(graph.UserCount())         // 2
	fmt.Println(graph.HasConnection(2, 3)) // false
	fmt.Println(graph.HasConnection(1, 3)) // false

	common := graph.CommonConnections(1, 2)
	fmt.Println("common:", len(common))

	suggestions := graph.SuggestConnections(1)
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
	fmt.Println(len(g2.CommonConnections(1, 2))) // 1
	fmt.Println(len(g2.SuggestConnections(1)))   // 1 (Dan)
	fmt.Println(len(g2.GetAllUsers()))           // 4

	g3 := NewGraph()
	g3.AddUser(1, "Alice")
	g3.AddUser(2, "Bob")
	g3.AddUser(3, "Charlie")

	g3.AddTypedConnection(1, 2, Friend{})
	g3.AddTypedConnection(1, 3, Follower{})
	g3.AddTypedConnection(2, 3, Blocked{})

	friends := g3.GetConnectionsByType(1, TypeFriend)
	fmt.Println("friends:", len(friends)) // 1

	if info, ok := g3.GetConnectionInfo(2, 3); ok {
		fmt.Printf("2→3: type=%s weight=%d\n", info.Type(), info.Weight())
	}

	// Проверка бага, который был в оригинале: после RemoveUser
	// типизированные связи не должны оставаться "зомби".
	g3.RemoveUser(3)
	if _, ok := g3.GetConnectionInfo(1, 3); ok {
		fmt.Println("BUG: typed edge 1→3 survived RemoveUser(3)")
	} else {
		fmt.Println("RemoveUser cleaned typed edges: ok")
	}
}
