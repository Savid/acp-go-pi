package pi

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// storeEntryPath is where one wrapper-owned source lands beneath a store root.
func storeEntryPath(root string, name string) string {
	return filepath.Join(root, sharedExtensionDirName, sharedExtensionDigest(sharedExtensionSources()), name)
}

func TestSharedExtensionDigestNamesOneSetOfSources(t *testing.T) {
	t.Parallel()

	sources := sharedExtensionSources()
	require.Len(t, sources, 3)
	require.Equal(t, sharedExtensionDigest(sources), sharedExtensionDigest(sharedExtensionSources()))

	// Contents, name, and length all join the digest, so no edited source can
	// be published under the directory the previous one named.
	edited := sharedExtensionSources()
	edited[0].contents = append([]byte("//\n"), edited[0].contents...)
	require.NotEqual(t, sharedExtensionDigest(sources), sharedExtensionDigest(edited))

	renamed := sharedExtensionSources()
	renamed[1].name += ".bak"
	require.NotEqual(t, sharedExtensionDigest(sources), sharedExtensionDigest(renamed))
}

func TestPublishSharedExtensionsPublishesOnceAndReuses(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	published, err := publishSharedExtensions(root)
	require.NoError(t, err)
	require.Equal(t, storeEntryPath(root, BridgeExtensionFileName), published.bridge)
	require.Equal(t, storeEntryPath(root, PathExtensionFileName), published.path)
	require.Equal(t, storeEntryPath(root, MCPExtensionFileName), published.mcp)

	for path, want := range map[string][]byte{
		published.bridge: bridgeExtensionSource,
		published.path:   pathExtensionSource,
		published.mcp:    mcpExtensionSource,
	} {
		contents, readErr := os.ReadFile(path) // #nosec G304 -- test temp dir.
		require.NoError(t, readErr)
		require.Equal(t, want, contents)
	}

	// No staging carrier survives a completed publish.
	entries, err := os.ReadDir(filepath.Dir(published.bridge))
	require.NoError(t, err)
	require.Len(t, entries, 3)

	before, err := os.Stat(published.bridge)
	require.NoError(t, err)

	again, err := publishSharedExtensions(root)
	require.NoError(t, err)
	require.Equal(t, published, again)

	after, err := os.Stat(published.bridge)
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after))
	require.Equal(t, before.ModTime(), after.ModTime())
}

func TestPublishSharedExtensionsRefusesEmptyRoot(t *testing.T) {
	t.Parallel()

	_, err := publishSharedExtensions("")
	require.ErrorContains(t, err, "shared extension store requires a root")
}

func TestPublishSharedExtensionsRefusesEntryThatDoesNotMatchTheBinary(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	entry := storeEntryPath(root, BridgeExtensionFileName)
	require.NoError(t, os.MkdirAll(filepath.Dir(entry), 0o700))
	require.NoError(t, os.WriteFile(entry, []byte("console.log('not ours')\n"), 0o400))

	_, err := publishSharedExtensions(root)
	require.ErrorContains(t, err, "does not match the source compiled into this binary")
}

func TestPublishSharedExtensionsRefusesUnexpectedFileTypes(t *testing.T) {
	t.Parallel()

	t.Run("entry is not a regular file", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		entry := storeEntryPath(root, BridgeExtensionFileName)
		require.NoError(t, os.MkdirAll(entry, 0o700))

		_, err := publishSharedExtensions(root)
		require.ErrorContains(t, err, "is not a regular file")
	})

	t.Run("store root is not a directory", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		target := filepath.Join(t.TempDir(), "elsewhere")
		require.NoError(t, os.MkdirAll(target, 0o700))
		require.NoError(t, os.Symlink(target, filepath.Join(root, sharedExtensionDirName)))

		_, err := publishSharedExtensions(root)
		require.ErrorContains(t, err, "is not a directory")
	})

	t.Run("digest directory is not a directory", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		dir := filepath.Dir(storeEntryPath(root, BridgeExtensionFileName))
		target := filepath.Join(t.TempDir(), "elsewhere")
		require.NoError(t, os.MkdirAll(target, 0o700))
		require.NoError(t, os.MkdirAll(filepath.Dir(dir), 0o700))
		require.NoError(t, os.Symlink(target, dir))

		_, err := publishSharedExtensions(root)
		require.ErrorContains(t, err, "is not a directory")
	})
}

func TestPublishSharedExtensionsSeamFailures(t *testing.T) {
	tests := []struct {
		name    string
		inject  func(t *testing.T, root string)
		message string
	}{
		{
			name: "create store",
			inject: func(_ *testing.T, _ string) {
				fsMkdirAll = func(string, os.FileMode) error { return os.ErrPermission }
			},
			message: "create shared extension store",
		},
		{
			name: "inspect store",
			inject: func(_ *testing.T, root string) {
				original := fsLstat
				fsLstat = func(path string) (fs.FileInfo, error) {
					if path == filepath.Join(root, sharedExtensionDirName) {
						return nil, os.ErrPermission
					}

					return original(path)
				}
			},
			message: "inspect shared extension store",
		},
		{
			name: "stage carrier",
			inject: func(_ *testing.T, _ string) {
				fsCreateTemp = func(string, string) (*os.File, error) { return nil, os.ErrPermission }
			},
			message: "stage shared extension",
		},
		{
			name: "write carrier",
			inject: func(t *testing.T, _ string) {
				t.Helper()

				original := fsCreateTemp
				fsCreateTemp = func(dir string, pattern string) (*os.File, error) {
					file, err := original(dir, pattern)
					require.NoError(t, err)
					require.NoError(t, file.Close())

					return file, nil
				}
			},
			message: "stage shared extension",
		},
		{
			name: "restrict carrier",
			inject: func(_ *testing.T, _ string) {
				fsChmod = func(string, os.FileMode) error { return os.ErrPermission }
			},
			message: "stage shared extension",
		},
		{
			name: "publish carrier",
			inject: func(_ *testing.T, _ string) {
				fsLink = func(string, string) error { return os.ErrPermission }
			},
			message: "publish shared extension",
		},
		{
			name: "clean carrier",
			inject: func(_ *testing.T, _ string) {
				fsRemove = func(string) error { return os.ErrPermission }
			},
			message: "clean shared extension staging",
		},
		{
			name: "inspect entry",
			inject: func(_ *testing.T, root string) {
				original := fsLstat
				fsLstat = func(path string) (fs.FileInfo, error) {
					if path == storeEntryPath(root, BridgeExtensionFileName) {
						return nil, os.ErrPermission
					}

					return original(path)
				}
			},
			message: "inspect shared extension",
		},
		{
			name: "read entry",
			inject: func(t *testing.T, root string) {
				t.Helper()

				entry := storeEntryPath(root, BridgeExtensionFileName)
				require.NoError(t, os.MkdirAll(filepath.Dir(entry), 0o700))
				require.NoError(t, os.WriteFile(entry, bridgeExtensionSource, 0o400))

				fsReadFile = func(string) ([]byte, error) { return nil, os.ErrPermission }
			},
			message: "read shared extension",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restoreAgentDirSeams(t)

			root := t.TempDir()
			test.inject(t, root)

			_, err := publishSharedExtensions(root)
			require.ErrorContains(t, err, test.message)
		})
	}
}

func TestPublishSharedExtensionLosesThePublishRace(t *testing.T) {
	t.Run("winner published the same bytes", func(t *testing.T) {
		restoreAgentDirSeams(t)

		root := t.TempDir()
		original := fsLink
		fsLink = func(oldname string, newname string) error {
			// Stand in for the process that published this name first.
			if err := original(oldname, newname); err != nil {
				return err
			}

			return fs.ErrExist
		}

		published, err := publishSharedExtensions(root)
		require.NoError(t, err)
		require.Equal(t, storeEntryPath(root, BridgeExtensionFileName), published.bridge)

		contents, err := os.ReadFile(published.bridge) // #nosec G304 -- test temp dir.
		require.NoError(t, err)
		require.Equal(t, bridgeExtensionSource, contents)
	})

	t.Run("winner published something else", func(t *testing.T) {
		restoreAgentDirSeams(t)

		root := t.TempDir()
		fsLink = func(_ string, newname string) error {
			if writeErr := os.WriteFile(newname, []byte("substituted\n"), 0o400); writeErr != nil {
				return writeErr
			}

			return fs.ErrExist
		}

		_, err := publishSharedExtensions(root)
		require.ErrorContains(t, err, "does not match the source compiled into this binary")
	})

	t.Run("carrier cleanup fails after the race", func(t *testing.T) {
		restoreAgentDirSeams(t)

		root := t.TempDir()
		original := fsLink
		fsLink = func(oldname string, newname string) error {
			if err := original(oldname, newname); err != nil {
				return err
			}

			return fs.ErrExist
		}
		fsRemove = func(string) error { return os.ErrPermission }

		_, err := publishSharedExtensions(root)
		require.ErrorIs(t, err, os.ErrPermission)
	})
}
