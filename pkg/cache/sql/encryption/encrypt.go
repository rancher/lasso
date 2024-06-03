/*
package encryption provides a struct, Manager, that generates a key-encryption-key, generates and rotates a
data-encryption-key, and provides functions for encryption and decryption. The key-encryption-key is used so the client
can store an encrypted version of their data key. This key-hierarchy enables the rotation of data-encryption-keys which
is intended to protect from cryptanalysis and mitigate impact of leaking a key.
*/

package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"sync"

	"github.com/pkg/errors"
)

const (
	maxWriteCount = 150000
	keySize       = 32 // 32 for AES-256
)

// Manager uses AES-GCM encryption with a key-hierarchy model. After a specified number of counts the key-encryption-key (KEK)
// is rotated.
type Manager struct {
	keyEncryptionKey  []byte      // main key
	dataEncryptionKey []byte      // current key
	keyGCMCipher      cipher.AEAD // cipher in gcm mode created from kek
	writesCounter     int
	lock              sync.Mutex
}

// NewManager returns Manager with a generated key-encryption-key which never rotates, and a data-encryption-key which does
// automatically rotate.
func NewManager() (*Manager, error) {
	kek, err := genRandKey()
	if err != nil {
		return nil, err
	}

	dek, err := genRandKey()
	if err != nil {
		return nil, err
	}

	keyGCMCipher, err := createGCMCypher(kek)
	if err != nil {
		return nil, err
	}

	return &Manager{
		keyEncryptionKey:  kek,
		dataEncryptionKey: dek,
		keyGCMCipher:      keyGCMCipher,
	}, nil

}

// Encrypt encrypts data using the Manager's current dataEncryptionKey. It then encrypts that key using the Manager's
// keyEncryptionKey. Encrypt returns the encrypted data, the nonce used to encrypt the data, the encrypted
// data-encryption-key,  and the nonce used with the key-encryption-key to encrypt the data-encryption-key on success.
func (m *Manager) Encrypt(data []byte) ([]byte, []byte, []byte, []byte, error) {
	dek, err := m.fetchUpToDateDataKey()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	aead, err := createGCMCypher(dek)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	edata, edatanonce, err := encrypt(aead, data)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	edek, edeknonce, err := m.encryptDataKey(dek)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return edata, edatanonce, edek, edeknonce, nil
}

// Decrypt accepts encrypted data, an encrypted data-encryption-key (edek), the nonce that was used to encrypt the data,
// and the nonce that was used to encrypt the data-encryption key. The edek is decrypted then used to decrpyt the data.
func (m *Manager) Decrypt(edata, datanonce, edek, deknonce []byte) ([]byte, error) {
	// We create block ciphers from the kek every time because there is no guarantee that block ciphers are safe for
	// concurrency.
	keyGCMCipher, err := createGCMCypher(m.keyEncryptionKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decrypt DEK with KEK")
	}
	dek, err := keyGCMCipher.Open(nil, deknonce, edek, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decrypt DEK")
	}
	aead, err := createGCMCypher(dek)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create GCMCypher from DEK")
	}
	data, err := aead.Open(nil, datanonce, edata, nil)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to decrypt data using DEK: %s", dek))
	}
	return data, nil
}

func (m *Manager) encryptDataKey(dek []byte) ([]byte, []byte, error) {
	return encrypt(m.keyGCMCipher, dek)
}

func encrypt(aead cipher.AEAD, data []byte) ([]byte, []byte, error) {
	if aead == nil {
		return nil, nil, fmt.Errorf("aead is nil, cannot encrypt data")
	}
	nonce, err := genRandByteSlice(aead.NonceSize())
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

func (m *Manager) isWriteCounterOverMax() bool {
	return m.writesCounter > maxWriteCount
}

// fetchUpToDateDataKey returns the current data key if writeCounter has not exceeded maxWriteCount. If it has then the
// data key is rotated. Before exiting the writesCounter is incremented by 1. This means that one use of the key has
// been accounted for. This is done so encryption does not block other processes from encrypting. The consequence is a
// key should only be used once and fetchUpToDateDataKey should be used to ensure an up-to-date key for any subsequent
// encryption.
func (m *Manager) fetchUpToDateDataKey() ([]byte, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.isWriteCounterOverMax() {
		m.writesCounter++
		return m.dataEncryptionKey, nil
	}

	err := m.rotateDataEncryptionKey()
	if err != nil {
		return nil, err
	}
	m.writesCounter++
	return m.dataEncryptionKey, nil
}

func (m *Manager) rotateDataEncryptionKey() error {
	dek, err := genRandKey()
	if err != nil {
		return err
	}
	m.dataEncryptionKey = dek
	return nil
}

func genRandKey() ([]byte, error) {
	return genRandByteSlice(keySize)
}

func genRandByteSlice(size int) ([]byte, error) {
	key := make([]byte, size)
	_, err := rand.Read(key)
	if err != nil {
		return nil, err
	}
	return key, nil
}
