def square_root_generator(limit):
  n = 1
  while n <= limit:
    yield n**0.5
    n += 1


# Example usage:
# limit = 5
limit = 13  # for q2 the value of 13th number
# 3.605551275463989
generator = square_root_generator(limit)

sum_sqrt_value = 0
for sqrt_value in generator:
  print(sqrt_value)
  sum_sqrt_value += sqrt_value
print(sum_sqrt_value)
# 8.382332347441762
