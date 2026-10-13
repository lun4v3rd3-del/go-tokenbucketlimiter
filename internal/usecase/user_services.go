package usecase

import (
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"reflect"
	"runtime"
	"strings"
)

type Identifier interface {
	Identify(req interface{}) (bool, error)
	GetKey(req interface{}) (string, error)
	GetGroupName() string // Добавили метод, чтобы UserService знал имя группы без парсинга строк
}

type VipIdentifier struct{ name string }

func NewVipIdentifier(name string) *VipIdentifier { return &VipIdentifier{name: name} }

func (i *VipIdentifier) GetGroupName() string { return i.name }

func (i *VipIdentifier) Identify(req interface{}) (bool, error) {
	httpRequest, ok := req.(*http.Request)
	if !ok {
		return false, nil
	}
	header := httpRequest.Header.Get("Authorization")
	return strings.Contains(strings.ToLower(header), "vip"), nil
}

func (i *VipIdentifier) GetKey(req interface{}) (string, error) {
	httpRequest, ok := req.(*http.Request)
	if !ok {
		return "", errors.New("invalid http request type in GetKey")
	}
	token := httpRequest.Header.Get("Authorization")

	return fmt.Sprintf("%s:%s", i.name, hashString(token)), nil
}

type DefaultIdentifier struct{ name string }

func NewDefaultIdentifier(name string) *DefaultIdentifier { return &DefaultIdentifier{name: name} }

func (i *DefaultIdentifier) GetGroupName() string { return i.name }

func (i *DefaultIdentifier) Identify(req interface{}) (bool, error) {
	_, ok := req.(*http.Request)
	return ok, nil
}

func (i *DefaultIdentifier) GetKey(req interface{}) (string, error) {
	httpRequest, ok := req.(*http.Request)
	if !ok {
		return "", errors.New("invalid http request type in GetKey")
	}
	ip := getClientIP(httpRequest)
	ua := httpRequest.Header.Get("User-Agent")
	fingerprint := fmt.Sprintf("%s:%s", ip, ua)

	return fmt.Sprintf("%s:%s", i.name, hashString(fingerprint)), nil
}

type UserService struct {
	Identifiers []Identifier
}

func NewUserService(identifiers []Identifier) *UserService {
	return &UserService{
		Identifiers: identifiers,
	}
}

func (s *UserService) GetUserKey(req interface{}) (string, error) {
	for _, identifier := range s.Identifiers {
		matched, err := identifier.Identify(req)
		if err != nil {
			return "", fmt.Errorf("identity check failed: %w", err)
		}
		if matched {
			key, err := identifier.GetKey(req)
			if err != nil {
				return "", fmt.Errorf("failed to generate key: %w", err)
			}
			return key, nil
		}
	}
	return "", errors.New("client could not be identified by any strategy")
}

func (s *UserService) GetGroupName(userKey string) string {
	if strings.Contains(userKey, "vip") {
		return "vip"
	}
	return "default"
}

func hashString(s string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum64())
}

func getClientIP(req *http.Request) string {
	if ip := req.Header.Get("X-Forwarded-For"); ip != "" {
		parts := strings.Split(ip, ",")
		return strings.TrimSpace(parts[0])
	}
	parts := strings.Split(req.RemoteAddr, ":")
	return parts[0]
}

func getFuncName(f interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}
