# Code Smells — Go/C++ Examples for KWDB

This reference covers common code smells with examples adapted for Go and C++,
the primary languages used in KWDB. Use these patterns when refactoring.

---

## 1. Long Function

```go
// BAD: 200-line function that does everything
func ProcessOrder(ctx context.Context, orderID int64) error {
    // 50 lines: fetch order
    // 30 lines: validate order
    // 40 lines: calculate pricing
    // 30 lines: update inventory
    // 50 lines: send notifications
}

// GOOD: Broken into focused functions
func ProcessOrder(ctx context.Context, orderID int64) error {
    order, err := fetchOrder(ctx, orderID)
    if err != nil {
        return err
    }
    if err := validateOrder(order); err != nil {
        return err
    }
    pricing := calculatePricing(order)
    if err := updateInventory(ctx, order); err != nil {
        return err
    }
    return sendNotifications(ctx, order, pricing)
}
```

```cpp
// BAD
Status ProcessOrder(Context* ctx, int64_t order_id) {
  // ...
}

// GOOD: extracted helpers
Status ProcessOrder(Context* ctx, int64_t order_id) {
  Order order;
  RETURN_IF_ERROR(FetchOrder(ctx, order_id, &order));
  RETURN_IF_ERROR(ValidateOrder(order));
  Pricing pricing;
  CalculatePricing(order, &pricing);
  RETURN_IF_ERROR(UpdateInventory(ctx, order));
  return SendNotifications(ctx, order, pricing);
}
```

---

## 2. Duplicated Code

```go
// BAD: Same discount logic in multiple places
func calcUserDiscount(user User) float64 {
    if user.Membership == "gold" {
        return user.Total * 0.2
    }
    if user.Membership == "silver" {
        return user.Total * 0.1
    }
    return 0
}

func calcOrderDiscount(order Order) float64 {
    if order.User.Membership == "gold" {
        return order.Total * 0.2
    }
    if order.User.Membership == "silver" {
        return order.Total * 0.1
    }
    return 0
}

// GOOD: Extract common logic
func membershipDiscountRate(membership string) float64 {
    rates := map[string]float64{"gold": 0.2, "silver": 0.1}
    return rates[membership]
}

func calcUserDiscount(user User) float64 {
    return user.Total * membershipDiscountRate(user.Membership)
}

func calcOrderDiscount(order Order) float64 {
    return order.Total * membershipDiscountRate(order.User.Membership)
}
```

---

## 3. Large Class / God Object

```go
// BAD: One type does everything
type UserManager struct{}
func (u *UserManager) CreateUser()     {}
func (u *UserManager) UpdateUser()     {}
func (u *UserManager) DeleteUser()     {}
func (u *UserManager) SendEmail()      {}
func (u *UserManager) GenerateReport() {}
func (u *UserManager) HandlePayment()  {}

// GOOD: Single responsibility
type UserService struct{}
func (s *UserService) Create(data UserData) error { /* ... */ }
func (s *UserService) Update(id int64, data UserData) error { /* ... */ }
func (s *UserService) Delete(id int64) error { /* ... */ }

type EmailService struct{}
func (s *EmailService) Send(to, subject, body string) error { /* ... */ }

type ReportService struct{}
func (s *ReportService) Generate(typ string, params map[string]any) (*Report, error) { /* ... */ }
```

```cpp
// BAD
class UserManager {
  void CreateUser();
  void UpdateUser();
  void DeleteUser();
  void SendEmail();
  void GenerateReport();
  void HandlePayment();
};

// GOOD
class UserService {
  Status Create(const UserData& data);
  Status Update(int64_t id, const UserData& data);
  Status Delete(int64_t id);
};

class EmailService {
  Status Send(const std::string& to, const std::string& subject,
              const std::string& body);
};
```

---

## 4. Long Parameter List

```go
// BAD: Too many parameters
func CreateUser(email, password, name string, age int, address, city, country, phone string) error

// GOOD: Group related parameters into a struct
type CreateUserRequest struct {
    Email    string
    Password string
    Name     string
    Age      int
    Address  string
    City     string
    Country  string
    Phone    string
}

func CreateUser(req CreateUserRequest) error
```

```cpp
// BAD
Status CreateUser(const std::string& email, const std::string& password,
                  const std::string& name, int age,
                  const std::string& address, const std::string& city,
                  const std::string& country, const std::string& phone);

// GOOD
struct CreateUserRequest {
  std::string email;
  std::string password;
  std::string name;
  int age = 0;
  std::string address;
  std::string city;
  std::string country;
  std::string phone;
};

Status CreateUser(const CreateUserRequest& req);
```

---

## 5. Feature Envy

```go
// BAD: Order uses User's data more than its own
type Order struct{ Total float64 }

func (o *Order) CalcDiscount(user User) float64 {
    if user.Membership == "gold" {
        return o.Total * 0.2
    }
    return 0
}

// GOOD: Move the logic to User
type User struct {
    Membership string
    AccountAge int
}

func (u *User) DiscountRate(total float64) float64 {
    if u.Membership == "gold" {
        return total * 0.2
    }
    if u.AccountAge > 365 {
        return total * 0.1
    }
    return 0
}

type Order struct{ Total float64 }

func (o *Order) CalcDiscount(user User) float64 {
    return o.Total * user.DiscountRate(o.Total)
}
```

---

## 6. Primitive Obsession

```go
// BAD: Using primitives for domain concepts
func SendEmail(to, subject, body string) error
func CreatePhone(country, number string) string

// GOOD: Use domain types
type Email struct{ value string }

func (e Email) String() string { return e.value }

func NewEmail(value string) (Email, error) {
    if !strings.Contains(value, "@") {
        return Email{}, errors.New("invalid email")
    }
    return Email{value: value}, nil
}

type PhoneNumber struct {
    Country string
    Number  string
}

func (p PhoneNumber) String() string {
    return fmt.Sprintf("%s-%s", p.Country, p.Number)
}
```

```cpp
// GOOD: Use strong types
class Email {
 public:
  static StatusOr<Email> Create(const std::string& value) {
    if (value.find('@') == std::string::npos) {
      return Status(ErrorCode::kInvalidArgument, "invalid email");
    }
    return Email(value);
  }
  const std::string& value() const { return value_; }
 private:
  explicit Email(std::string value) : value_(std::move(value)) {}
  std::string value_;
};
```

---

## 7. Magic Numbers / Strings

```go
// BAD: Unexplained values
if user.Status == 2 { /* ... */ }
discount := total * 0.15
time.AfterFunc(86400000*time.Millisecond, callback)

// GOOD: Named constants
const (
    UserStatusActive   = 1
    UserStatusInactive = 2
    UserStatusSuspended = 3
)

const (
    DiscountStandard = 0.1
    DiscountPremium  = 0.15
    DiscountVIP      = 0.2
)

const OneDay = 24 * time.Hour

if user.Status == UserStatusInactive { /* ... */ }
discount := total * DiscountPremium
time.AfterFunc(OneDay, callback)
```

```cpp
// BAD
if (user.status == 2) {}
const double discount = total * 0.15;

// GOOD: Use constants or enums
enum class UserStatus : int8_t { kActive = 1, kInactive = 2, kSuspended = 3 };
constexpr double kDiscountPremium = 0.15;
constexpr auto kOneDay = std::chrono::hours(24);

if (user.status == UserStatus::kInactive) {}
const double discount = total * kDiscountPremium;
```

---

## 8. Nested Conditionals (Arrow Code)

```go
// BAD: Deeply nested
func process(order *Order) *Result {
    if order != nil {
        if order.User != nil {
            if order.User.IsActive {
                if order.Total > 0 {
                    return processOrder(order)
                } else {
                    return &Result{Error: "Invalid total"}
                }
            } else {
                return &Result{Error: "User inactive"}
            }
        } else {
            return &Result{Error: "No user"}
        }
    } else {
        return &Result{Error: "No order"}
    }
}

// GOOD: Guard clauses / early returns
func process(order *Order) *Result {
    if order == nil {
        return &Result{Error: "No order"}
    }
    if order.User == nil {
        return &Result{Error: "No user"}
    }
    if !order.User.IsActive {
        return &Result{Error: "User inactive"}
    }
    if order.Total <= 0 {
        return &Result{Error: "Invalid total"}
    }
    return processOrder(order)
}
```

```cpp
// GOOD with early returns
Status Process(const Order& order) {
  RETURN_IF_ERROR(ValidateOrderExists(order));
  RETURN_IF_ERROR(ValidateUserExists(order));
  RETURN_IF_ERROR(ValidateUserActive(order.user()));
  RETURN_IF_ERROR(ValidateOrderTotal(order));
  return ProcessOrder(order);
}
```

---

## 9. Dead Code

```go
// BAD: Unused code lingers
func oldImplementation() {}       // never called
const DeprecatedValue = 5         // never used
import "old/deprecated"           // unused import
// commentedOutCode()             // git history has it

// GOOD: Remove it. Git history preserves everything.
```

```cpp
// BAD
void OldImplementation();  // never called
constexpr int kDeprecated = 5;  // never used

// GOOD: Delete unused declarations, includes, and comments.
```

---

## 10. Inappropriate Intimacy

```go
// BAD: Breaking encapsulation
func (p *OrderProcessor) Process(order *Order) {
    street := order.User.Profile.Address.Street  // too intimate
    cfg := order.Repository.Conn.Config          // breaking encapsulation
}

// GOOD: Ask, don't tell
func (p *OrderProcessor) Process(order *Order) {
    address := order.GetShippingAddress()  // Order knows how to get it
    order.Save()                           // Order knows how to save itself
}
```

---

## 11. Shotgun Surgery (KWDB-specific)

When a single change requires touching many files, consider consolidating:

```go
// BAD: Same constant duplicated across files
// file_a.go: const maxRetries = 3
// file_b.go: const maxRetries = 3
// file_c.go: const maxRetries = 3

// GOOD: Single source of truth
// retry.go: const MaxRetries = 3
```

---

## 12. Speculative Generality

```go
// BAD: Abstraction that isn't needed yet
type DataProcessor interface {
    Process(ctx context.Context, data any) (any, error)
    // Only one implementation exists, and no second one is planned
}

// GOOD: Concrete until proven otherwise
func processData(ctx context.Context, data *Input) (*Output, error)
```

---

## Summary: Quick Reference

| Smell | Symptom | Fix |
|---|---|---|
| Long function | >50 lines, multiple responsibilities | Extract method |
| Duplicated code | Same logic repeated | Extract function / DRY |
| God object | Type does unrelated things | Split by responsibility |
| Long parameter list | 4+ parameters | Introduce parameter struct |
| Feature envy | Method uses another type's data more | Move method to data owner |
| Primitive obsession | Strings/ints for domain concepts | Create domain types |
| Magic numbers | Unexplained literal values | Named constants |
| Arrow code | Deep nesting (3+ levels) | Guard clauses / early returns |
| Dead code | Unused functions, imports, comments | Delete |
| Inappropriate intimacy | Breaking encapsulation | Ask, don't tell |
