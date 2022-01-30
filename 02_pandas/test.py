import pandas as pd
x = pd.DataFrame({'A': [4, 6, 4, 5], 'B': ['e', 'c', 'e', 'c']})

print(f'не foo: {x}')


def foo(x):
    print('foo')
    print(x.mean())

# x['A'].apply(foo)
print(x['A'].mean())

x.agg