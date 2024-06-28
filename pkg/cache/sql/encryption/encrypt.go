// package encryption provides encryption and decryption functions, while
// abstracting away key management concerns.
package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var (
	ErrKeyNotFound = errors.New("data key not found")
	ErrInvalidKey  = errors.New("invalid key")

	maxWriteCount int32 = 150000
)

const (
	keySize = 32 // 32 for AES-256
)

// Manager uses AES-GCM encryption and keeps in memory the data encryption
// keys. The active encryption key is automatically rotated once it has been
// used over a certain amount of times - defined by maxWriteCount.
type Manager struct {
	dataKeys         map[uuid.UUID][]byte
	activeKey        uuid.UUID
	activeKeyCounter int32

	// lock works as the mutual exclusion lock for dataKeys, activeKey
	// and the activeKeyCounter.
	lock sync.Mutex
}

// NewManager returns Manager.
func NewManager() (*Manager, error) {
	dek := make([]byte, keySize, keySize)
	err := newRand(dek)
	if err != nil {
		return nil, err
	}

	m := &Manager{
		dataKeys: map[uuid.UUID][]byte{},
	}

	keyID, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	m.activeKey = keyID
	m.dataKeys[m.activeKey] = dek

	return m, nil
}

// Encrypt encrypts data using the current active key.
// Returned values are: the encrypted data, the nonce used to encrypt the
// data, and the key ID used to encrypt the data.
func (m *Manager) Encrypt(data []byte) ([]byte, []byte, uuid.UUID, error) {
	dek, keyID, err := m.fetchActiveDataKey()
	if err != nil {
		return nil, nil, uuid.Nil, err
	}
	aead, err := createGCMCypher(dek)
	if err != nil {
		return nil, nil, uuid.Nil, err
	}
	edata, nonce, err := encrypt(aead, data)
	if err != nil {
		return nil, nil, uuid.Nil, err
	}
	return edata, nonce, keyID, nil
}

// Decrypt decrypts a pair of encrypted data and nonce based on a keyID.
func (m *Manager) Decrypt(edata, nonce []byte, keyID uuid.UUID) ([]byte, error) {
	dek, ok := m.dataKeys[keyID]
	if !ok {
		return nil, fmt.Errorf("%w: %v", ErrKeyNotFound, keyID)
	}

	aead, err := createGCMCypher(dek)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create GCMCypher from DEK")
	}
	data, err := aead.Open(nil, nonce, edata, nil)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to decrypt data using %s", keyID))
	}
	return data, nil
}

func encrypt(aead cipher.AEAD, data []byte) ([]byte, []byte, error) {
	if aead == nil {
		return nil, nil, fmt.Errorf("aead is nil, cannot encrypt data")
	}
	nonce := make([]byte, aead.NonceSize())
	err := newRand(nonce)
	if err != nil {
		return nil, nil, err
	}
	sealed := aead.Seal(nil, nonce, data, nil)
	return sealed, nonce, nil
}

func createGCMCypher(key []byte) (cipher.AEAD, error) {
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(b)
	if err != nil {
		return nil, err
	}
	return aead, nil
}

// fetchActiveDataKey returns the current data key and its key ID.
// Each call results in activeKeyCounter being incremented by 1. When the
// the activeKeyCounter exceeds maxWriteCount, the active data key is
// rotated - before being returned.
func (m *Manager) fetchActiveDataKey() ([]byte, uuid.UUID, error) {
	m.lock.Lock()
	m.activeKeyCounter++

	if m.activeKey == uuid.Nil || m.activeKeyCounter >= maxWriteCount {
		dek, keyID, err := m.newDataEncryptionKey()
		if err != nil {
			m.lock.Unlock()
			return nil, uuid.Nil, err
		}

		m.activeKey = keyID
		m.dataKeys[m.activeKey] = dek
		m.lock.Unlock()
		return dek, keyID, nil
	}
	dek, ok := m.dataKeys[m.activeKey]
	m.lock.Unlock()

	if !ok {
		return nil, uuid.Nil, fmt.Errorf("%w: %v", ErrKeyNotFound, m.activeKey)
	}

	return dek, m.activeKey, nil
}

func (m *Manager) newDataEncryptionKey() ([]byte, uuid.UUID, error) {
	dek := make([]byte, keySize)
	err := newRand(dek)
	if err != nil {
		return nil, uuid.Nil, err
	}

	keyID, err := uuid.NewRandom()
	if err != nil {
		return nil, uuid.Nil, err
	}

	return dek, keyID, nil
}

func newRand(d []byte) error {
	_, err := rand.Read(d)
	if err != nil {
		return err
	}

	return nil
}
