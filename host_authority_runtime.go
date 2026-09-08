package piacp

import (
	"context"
	"errors"
	"reflect"
)

func hostAuthorityNil(authority HostAuthority) bool {
	if authority == nil {
		return true
	}

	value := reflect.ValueOf(authority)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (a *Agent) disposeNativeTree(ctx context.Context, root string) error {
	if root == "" {
		return nil
	}

	if a.options.hostAuthoritySupplied {
		if err := a.reclaimNativeTree(ctx, root); err != nil {
			return err
		}
	}

	return materializeRemoveAll(root)
}

func (a *Agent) removeNativeTree(root string) error {
	if root == "" {
		return nil
	}

	return materializeRemoveAll(root)
}

func readHostEnvironment(authority HostAuthority) (environment map[string]string, err error) {
	if hostAuthorityNil(authority) {
		return nil, ErrHostAuthorityUnavailable
	}

	defer func() {
		if recover() != nil {
			environment = nil
			err = ErrHostAuthorityUnavailable
		}
	}()

	environment = authority.NativeEnvironment()
	if environment == nil {
		return nil, ErrHostAuthorityUnavailable
	}

	return cloneStringMap(environment), nil
}

func (a *Agent) prepareNativeTree(ctx context.Context, root string) (err error) {
	if !a.options.hostAuthoritySupplied {
		return nil
	}

	defer func() {
		if recover() != nil {
			err = errors.Join(ErrHostAuthorityUnavailable, ErrContainmentIncomplete)
		}

		a.recordNativeContainment(err)
	}()

	a.managedHandoff.freeze()

	err = a.options.HostAuthority.PrepareNativeTree(ctx, root)
	if err != nil {
		err = errors.Join(err, ErrContainmentIncomplete)
	}

	return err
}

func (a *Agent) reclaimNativeTree(ctx context.Context, root string) (err error) {
	if !a.options.hostAuthoritySupplied {
		return nil
	}

	defer func() {
		if recover() != nil {
			err = errors.Join(ErrHostAuthorityUnavailable, ErrContainmentIncomplete)
		}

		a.recordNativeContainment(err)
	}()

	err = a.options.HostAuthority.ReclaimNativeTree(ctx, root)
	if errors.Is(err, ErrNativeTreeBusy) {
		a.markNativeTreeBusy(root)
	} else if err == nil {
		a.clearNativeTreeBusy(root)
	}

	if err != nil && !errors.Is(err, ErrNativeTreeBusy) {
		err = errors.Join(err, ErrContainmentIncomplete)
	}

	return err
}

func nativeProcessNil(process NativeProcess) bool {
	if process == nil {
		return true
	}

	value := reflect.ValueOf(process)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func nativeContainmentComplete(err error) bool {
	return !errors.Is(err, ErrContainmentIncomplete) && !errors.Is(err, ErrHostAuthorityUnavailable)
}
