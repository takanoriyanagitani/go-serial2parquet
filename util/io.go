package util

import (
	"context"
	"database/sql"
)

type IO[T any] func(context.Context) (T, error)

func (i IO[T]) Or(alt IO[T]) IO[T] {
	return func(ctx context.Context) (T, error) {
		t, e := i(ctx)
		switch e {
		case nil:
			return t, nil
		default:
			return alt(ctx)
		}
	}
}

func (i IO[T]) Must(ctx context.Context) T {
	t, e := i(ctx)
	if nil != e {
		panic(e)
	}
	return t
}

func (i IO[T]) ToAny() IO[any] {
	return func(ctx context.Context) (any, error) { return i(ctx) }
}

func (i IO[T]) ToString(conv func(T) string) IO[string] {
	return Bind(
		i,
		Lift(func(t T) (string, error) {
			return conv(t), nil
		}),
	)
}

func Ok[T any](result IO[T]) IO[sql.Null[T]] {
	return func(ctx context.Context) (ret sql.Null[T], e error) {
		t, e := result(ctx)
		switch e {
		case nil:
			return sql.Null[T]{V: t, Valid: true}, nil
		default:
			return ret, nil
		}
	}
}

func Err[T any](err error) IO[T] {
	return func(_ context.Context) (t T, e error) {
		return t, err
	}
}

func Of[T any](t T) IO[T] {
	return func(_ context.Context) (T, error) {
		return t, nil
	}
}

func OfFn[T any](f func() T) IO[T] {
	return func(_ context.Context) (T, error) {
		return f(), nil
	}
}

func Bind[T, U any](
	i IO[T],
	f func(T) IO[U],
) IO[U] {
	return func(ctx context.Context) (u U, e error) {
		t, e := i(ctx)
		if nil != e {
			return u, e
		}
		return f(t)(ctx)
	}
}

func Lift[T, U any](
	pure func(T) (U, error),
) func(T) IO[U] {
	return func(t T) IO[U] {
		return func(_ context.Context) (U, error) {
			return pure(t)
		}
	}
}

type Void struct{}

var Empty Void = struct{}{}

func All[T any](i ...IO[T]) IO[[]T] {
	return func(ctx context.Context) ([]T, error) {
		var ret []T = make([]T, 0, len(i))
		for _, iot := range i {
			t, e := iot(ctx)
			if nil != e {
				return nil, e
			}
			ret = append(ret, t)
		}
		return ret, nil
	}
}
