package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewManager(t *testing.T) {
	m, err := NewManager()
	if err != nil {
		t.FailNow()
	}
	assert.NotNil(t, m)
}

func TestEncrypt(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}
	var tests []testCase

	tests = append(tests, testCase{description: "test encrypt with no errors", test: func(t *testing.T) {
		testKEK := []byte{208, 54, 21, 118, 74, 213, 2, 151, 38, 199, 225, 46, 189, 55, 60, 91}
		testDEK := []byte{83, 125, 203, 18, 75, 156, 24, 192, 119, 73, 157, 222, 143, 140, 231, 181}

		m := Manager{
			keyEncryptionKey:  testKEK,
			dataEncryptionKey: testDEK,
		}
		b, err := aes.NewCipher(m.keyEncryptionKey)
		assert.Nil(t, err)
		aead, err := cipher.NewGCM(b)
		assert.Nil(t, err)
		m.keyGCMCipher = aead

		testData := []byte("something")
		encryptedData, dataNonce, encryptedDEK, dekNonce, err := m.Encrypt(testData)
		assert.Nil(t, err)

		decryptedDEK, err := aead.Open(nil, dekNonce, encryptedDEK, nil)
		assert.Nil(t, err)
		assert.Equal(t, m.dataEncryptionKey, decryptedDEK)

		b, err = aes.NewCipher(m.dataEncryptionKey)
		assert.Nil(t, err)
		aead, err = cipher.NewGCM(b)
		assert.Nil(t, err)
		decryptedData, err := aead.Open(nil, dataNonce, encryptedData, nil)
		assert.Nil(t, err)
		assert.Equal(t, testData, decryptedData)
	}})
	tests = append(tests, testCase{description: "test encrypt with nil aead", test: func(t *testing.T) {
		testKEK := []byte{208, 54, 21, 118, 74, 213, 2, 151, 38, 199, 225, 46, 189, 55, 60, 91}
		testDEK := []byte{83, 125, 203, 18, 75, 156, 24, 192, 119, 73, 157, 222, 143, 140, 231, 181}

		m := Manager{
			keyEncryptionKey:  testKEK,
			dataEncryptionKey: testDEK,
		}

		testData := []byte("something")
		_, _, _, _, err := m.Encrypt(testData)
		assert.NotNil(t, err)

	}})
	tests = append(tests, testCase{description: "test encrypt with nil KEK and DEK", test: func(t *testing.T) {
		var testDEK []byte
		testKEK := []byte{208, 54, 21, 118, 74, 213, 2, 151, 38, 199, 225, 46, 189, 55, 60, 91}
		m := Manager{
			keyEncryptionKey:  testKEK,
			dataEncryptionKey: testDEK,
		}

		b, err := aes.NewCipher([]byte{208, 54, 21, 118, 74, 213, 2, 151, 38, 199, 225, 46, 189, 55, 60, 91})
		assert.Nil(t, err)
		aead, err := cipher.NewGCM(b)
		assert.Nil(t, err)
		m.keyGCMCipher = aead

		testData := []byte("something")
		_, _, _, _, err = m.Encrypt(testData)
		assert.NotNil(t, err)

	}})
	tests = append(tests, testCase{description: "test encrypt with key rotation", test: func(t *testing.T) {
		testKEK := []byte{208, 54, 21, 118, 74, 213, 2, 151, 38, 199, 225, 46, 189, 55, 60, 91}
		testDEK := []byte{83, 125, 203, 18, 75, 156, 24, 192, 119, 73, 157, 222, 143, 140, 231, 181}

		m := Manager{
			keyEncryptionKey:  testKEK,
			dataEncryptionKey: testDEK,
			writesCounter:     maxWriteCount + 1,
		}
		b, err := aes.NewCipher(m.keyEncryptionKey)
		assert.Nil(t, err)
		aead, err := cipher.NewGCM(b)
		assert.Nil(t, err)
		m.keyGCMCipher = aead

		testData := []byte("something")
		encryptedData, dataNonce, encryptedDEK, dekNonce, err := m.Encrypt(testData)
		assert.Nil(t, err)

		decryptedDEK, err := aead.Open(nil, dekNonce, encryptedDEK, nil)
		assert.Nil(t, err)
		assert.NotEqual(t, testDEK, decryptedDEK)
		assert.Equal(t, m.dataEncryptionKey, decryptedDEK)

		b, err = aes.NewCipher(m.dataEncryptionKey)
		assert.Nil(t, err)
		aead, err = cipher.NewGCM(b)
		assert.Nil(t, err)
		decryptedData, err := aead.Open(nil, dataNonce, encryptedData, nil)
		assert.Nil(t, err)
		assert.Equal(t, testData, decryptedData)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestDecrypt(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}
	var tests []testCase

	tests = append(tests, testCase{description: "test decrypt with no errors", test: func(t *testing.T) {
		testKEK := []byte{208, 54, 21, 118, 74, 213, 2, 151, 38, 199, 225, 46, 189, 55, 60, 91}
		testDEK := []byte{83, 125, 203, 18, 75, 156, 24, 192, 119, 73, 157, 222, 143, 140, 231, 181}

		m := Manager{
			keyEncryptionKey:  testKEK,
			dataEncryptionKey: testDEK,
		}
		// encrypt dek
		b, err := aes.NewCipher(testKEK)
		assert.Nil(t, err)
		aead, err := cipher.NewGCM(b)
		assert.Nil(t, err)
		m.keyGCMCipher = aead
		dekNonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(dekNonce)
		assert.Nil(t, err)
		encryptedDEK := aead.Seal(nil, dekNonce, testDEK, nil)

		// encrypt data
		testData := []byte("something")
		b, err = aes.NewCipher(testDEK)
		assert.Nil(t, err)
		dekAEAD, err := cipher.NewGCM(b)
		assert.Nil(t, err)
		dataNonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(dataNonce)
		assert.Nil(t, err)
		encryptedData := dekAEAD.Seal(nil, dataNonce, testData, nil)

		// decrypted encrypted data using encrypted dek
		decryptedData, err := m.Decrypt(encryptedData, dataNonce, encryptedDEK, dekNonce)
		assert.Nil(t, err)
		assert.Equal(t, testData, decryptedData)
	},
	})
	tests = append(tests, testCase{description: "test decrypt with nil kek aead- should still work, aead not needed for decrypt since inputs are passed", test: func(t *testing.T) {
		testKEK := []byte{208, 54, 21, 118, 74, 213, 2, 151, 38, 199, 225, 46, 189, 55, 60, 91}
		testDEK := []byte{83, 125, 203, 18, 75, 156, 24, 192, 119, 73, 157, 222, 143, 140, 231, 181}

		m := Manager{
			keyEncryptionKey:  testKEK,
			dataEncryptionKey: testDEK,
		}
		// encrypt dek
		b, err := aes.NewCipher(testKEK)
		assert.Nil(t, err)
		aead, err := cipher.NewGCM(b)
		assert.Nil(t, err)

		dekNonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(dekNonce)
		assert.Nil(t, err)
		encryptedDEK := aead.Seal(nil, dekNonce, testDEK, nil)

		// encrypt data
		testData := []byte("something")
		b, err = aes.NewCipher(testDEK)
		assert.Nil(t, err)
		dekAEAD, err := cipher.NewGCM(b)
		assert.Nil(t, err)
		dataNonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(dataNonce)
		assert.Nil(t, err)
		encryptedData := dekAEAD.Seal(nil, dataNonce, testData, nil)

		// decrypted encrypted data using encrypted dek
		decryptedData, err := m.Decrypt(encryptedData, dataNonce, encryptedDEK, dekNonce)
		assert.Nil(t, err)
		assert.Equal(t, testData, decryptedData)
	},
	})
	tests = append(tests, testCase{description: "test encrypt with nil DEK and KEK", test: func(t *testing.T) {
		testKEK := []byte{208, 54, 21, 118, 74, 213, 2, 151, 38, 199, 225, 46, 189, 55, 60, 91}
		testDEK := []byte{83, 125, 203, 18, 75, 156, 24, 192, 119, 73, 157, 222, 143, 140, 231, 181}

		m := Manager{}
		// encrypt dek
		b, err := aes.NewCipher(testKEK)
		assert.Nil(t, err)
		aead, err := cipher.NewGCM(b)
		assert.Nil(t, err)

		dekNonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(dekNonce)
		assert.Nil(t, err)
		encryptedDEK := aead.Seal(nil, dekNonce, testDEK, nil)

		// encrypt data
		testData := []byte("something")
		b, err = aes.NewCipher(testDEK)
		assert.Nil(t, err)
		dekAEAD, err := cipher.NewGCM(b)
		assert.Nil(t, err)
		dataNonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(dataNonce)
		assert.Nil(t, err)
		encryptedData := dekAEAD.Seal(nil, dataNonce, testData, nil)

		// decrypted encrypted data using encrypted dek
		_, err = m.Decrypt(encryptedData, dataNonce, encryptedDEK, dekNonce)
		assert.NotNil(t, err)

	}})
	tests = append(tests, testCase{description: "test decrypt with wrong data nonce should return error", test: func(t *testing.T) {
		testKEK := []byte{208, 54, 21, 118, 74, 213, 2, 151, 38, 199, 225, 46, 189, 55, 60, 91}
		testDEK := []byte{83, 125, 203, 18, 75, 156, 24, 192, 119, 73, 157, 222, 143, 140, 231, 181}

		m := Manager{
			keyEncryptionKey:  testKEK,
			dataEncryptionKey: testDEK,
		}
		// encrypt dek
		b, err := aes.NewCipher(testKEK)
		assert.Nil(t, err)
		aead, err := cipher.NewGCM(b)
		assert.Nil(t, err)
		m.keyGCMCipher = aead
		dekNonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(dekNonce)
		assert.Nil(t, err)
		encryptedDEK := aead.Seal(nil, dekNonce, testDEK, nil)

		// encrypt data
		testData := []byte("something")
		b, err = aes.NewCipher(testDEK)
		assert.Nil(t, err)
		dekAEAD, err := cipher.NewGCM(b)
		assert.Nil(t, err)
		dataNonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(dataNonce)
		assert.Nil(t, err)
		encryptedData := dekAEAD.Seal(nil, dataNonce, testData, nil)

		// generate random nonce not used to seal either dek or data
		randomNonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(dataNonce)
		assert.Nil(t, err)
		// decrypted encrypted data using encrypted dek
		_, err = m.Decrypt(encryptedData, randomNonce, encryptedDEK, dekNonce)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "test decrypt with wrong dek nonce should return error", test: func(t *testing.T) {
		testKEK := []byte{208, 54, 21, 118, 74, 213, 2, 151, 38, 199, 225, 46, 189, 55, 60, 91}
		testDEK := []byte{83, 125, 203, 18, 75, 156, 24, 192, 119, 73, 157, 222, 143, 140, 231, 181}

		m := Manager{
			keyEncryptionKey:  testKEK,
			dataEncryptionKey: testDEK,
		}
		// encrypt dek
		b, err := aes.NewCipher(testKEK)
		assert.Nil(t, err)
		aead, err := cipher.NewGCM(b)
		assert.Nil(t, err)
		m.keyGCMCipher = aead
		dekNonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(dekNonce)
		assert.Nil(t, err)
		encryptedDEK := aead.Seal(nil, dekNonce, testDEK, nil)

		// encrypt data
		testData := []byte("something")
		b, err = aes.NewCipher(testDEK)
		assert.Nil(t, err)
		dekAEAD, err := cipher.NewGCM(b)
		assert.Nil(t, err)
		dataNonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(dataNonce)
		assert.Nil(t, err)
		encryptedData := dekAEAD.Seal(nil, dataNonce, testData, nil)

		// generate random nonce not used to seal either dek or data
		randomNonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(dataNonce)
		assert.Nil(t, err)
		// decrypted encrypted data using encrypted dek
		_, err = m.Decrypt(encryptedData, dataNonce, encryptedDEK, randomNonce)
		assert.NotNil(t, err)
	},
	})

	tests = append(tests, testCase{description: "test decrypt with DEK/nonce pair not used to encrypt should return error", test: func(t *testing.T) {
		testKEK := []byte{208, 54, 21, 118, 74, 213, 2, 151, 38, 199, 225, 46, 189, 55, 60, 91}
		testDEK := []byte{83, 125, 203, 18, 75, 156, 24, 192, 119, 73, 157, 222, 143, 140, 231, 181}

		m := Manager{
			keyEncryptionKey:  testKEK,
			dataEncryptionKey: testDEK,
		}
		// set aead
		b, err := aes.NewCipher(testKEK)
		assert.Nil(t, err)
		aead, err := cipher.NewGCM(b)
		assert.Nil(t, err)
		m.keyGCMCipher = aead

		// encrypt data
		testData := []byte("something")
		b, err = aes.NewCipher(testDEK)
		assert.Nil(t, err)
		dekAEAD, err := cipher.NewGCM(b)
		assert.Nil(t, err)
		dataNonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(dataNonce)
		assert.Nil(t, err)
		encryptedData := dekAEAD.Seal(nil, dataNonce, testData, nil)

		// create and encrypt dek not used for encrypting data
		otherDEK := []byte{83, 125, 203, 18, 75, 156, 24, 192, 119, 73, 157, 222, 143, 145, 231, 181}

		// encrypt dek
		b, err = aes.NewCipher(testKEK)
		assert.Nil(t, err)
		otherAEAD, err := cipher.NewGCM(b)
		assert.Nil(t, err)
		otherDEKNonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(otherDEKNonce)
		assert.Nil(t, err)
		otherEncryptedDEK := otherAEAD.Seal(nil, otherDEKNonce, otherDEK, nil)

		// decrypted encrypted data using encrypted dek
		_, err = m.Decrypt(encryptedData, dataNonce, otherEncryptedDEK, otherDEKNonce)
		assert.NotNil(t, err)
	},
	})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}
