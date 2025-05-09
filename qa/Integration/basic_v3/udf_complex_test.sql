drop database if exists test_complex_udf;
create database test_complex_udf;
use test_complex_udf;

CREATE FUNCTION fibonacci(n int)
    RETURNs int
    LANGUAGE LUA
BEGIN
'
function fibonacci(n)
    if n <= 0 then
        return 0
    elseif n == 1 then
        return 1
    else
        return fibonacci(n-1) + fibonacci(n-2)
    end
end

for i = 1, 10 do
    print(fibonacci(i))
end

function factorial(n)
    if n == 0 then
        return 1
    else
        return n * factorial(n-1)
    end
end

for i = 1, 10 do
    print(factorial(i))
end
'
END;


CREATE FUNCTION fibonacci1(n int)
    RETURNs int
    LANGUAGE LUA
BEGIN
'
function fibonacci1(n)
    if n == 0 then
        return 0
    elseif n == 1 then
        return 1
    else
        local a, b = 0, 1
        for i = 2, n do
            local temp = a + b
            a = b
            b = temp
        end
        return b
    end
end
'
END;

select fibonacci1(10);


CREATE FUNCTION calculate_factorial(n int)
    RETURNs int
    LANGUAGE LUA
BEGIN
'
function calculate_factorial(n)
    return calculate_factorial_helper(n, 1)
end

function calculate_factorial_helper(n, acc)
    if n == 0 then
        return acc
    else
        return calculate_factorial_helper(n - 1, acc * n)
    end
end

local fact_5 = calculate_factorial(5)
print("Factorial of 5 is: " .. fact_5)
'
END;



CREATE FUNCTION calculate_difference(n int)
    RETURNs int
    LANGUAGE LUA
BEGIN
'
function factorial(n)
    if n == 0 then
        return 1
    else
        return n * factorial(n - 1)
    end
end

function square(x)
    return x * x
end

function cube(x)
    return x * x * x
end

function calculate_difference(n)
    local sum_of_squares = 0
    local sum_of_cubes = 0
    
    for i = 1, n do
        sum_of_squares = sum_of_squares + square(i)
        sum_of_cubes = sum_of_cubes + cube(i)
    end
    
    return sum_of_cubes - sum_of_squares
end

function calculate_factorial_square(m)
    local result = factorial(m)
    return square(result)
end

local input_number = 5
local diff_result = calculate_difference(input_number)
print("Difference between sum of squares and sum of cubes: " .. diff_result)

local fact_square = calculate_factorial_square(4)
print("Factorial of 4 squared: " .. fact_square)
'
END;


CREATE FUNCTION average_of_fibonacci(n int)
    RETURNs int
    LANGUAGE LUA
BEGIN
'
function fibonacci(n)
    if n <= 1 then
        return n
    else
        return fibonacci(n - 1) + fibonacci(n - 2)
    end
end

function sum_of_fibonacci(n)
    local sum = 0
    for i = 1, n do
        sum = sum + fibonacci(i)
    end
    return sum
end

function average_of_fibonacci(n)
    local sum = sum_of_fibonacci(n)
    return sum / n
end

local n = 5
local fib_n = fibonacci(n)
print("Fibonacci number at position " .. n .. ": " .. fib_n)

local sum_fib = sum_of_fibonacci(n)
print("Sum of first " .. n .. " Fibonacci numbers: " .. sum_fib)

local avg_fib = average_of_fibonacci(n)
print("Average of first " .. n .. " Fibonacci numbers: " .. avg_fib)
'
END;


CREATE FUNCTION reverse_string(s varchar)
    RETURNs varchar
    LANGUAGE LUA
BEGIN
'
function reverse_string(str)
  local len = string.len(str)
  local reversed = ""
  for i = len, 1, -1 do
    reversed = reversed .. string.sub(str, i, i)
  end
  return reversed
end
'
END;

select reverse_string('abcdefg');


CREATE FUNCTION count_character(str varchar, substr varchar)
    RETURNs int
    LANGUAGE LUA
BEGIN
'
function count_character(str, char)
  local count = 0
  for i = 1, string.len(str) do
    if string.sub(str, i, i) == char then
      count = count + 1
    end
  end
  return count
end
'
END;

select count_character('abcdefg','e');


CREATE FUNCTION is_prime(n int)
    RETURNs int
    LANGUAGE LUA
BEGIN
'
function is_prime(num)
  if num <= 1 then
    return 0
  end
  for i = 2, math.sqrt(num) do
    if num % i == 0 then
      return 0
    end
  end
  return 1
end
'
END;


select is_prime(24534631);


CREATE FUNCTION is_over_100_years(ts timestamp)
    RETURNs int
    LANGUAGE LUA
BEGIN
'
function is_over_100_years(timestamp)
  local referenceTimestamp = os.time({year=2000, month=1, day=1, hour=0, min=0, sec=0})
  local diffYears = (os.difftime(timestamp, referenceTimestamp)) / (60 * 60 * 24 * 365.25)
  if diffYears > 100 then
    return 0
  else
    return 1
  end
end
'
END;

select is_over_100_years('2120-02-02 00:00:00');


CREATE FUNCTION capitalize_words(sentence nvarchar)
    RETURNs nchar
    LANGUAGE LUA
BEGIN
'
function capitalize_words(sentence)
    function capitalizeWord(word)
        local firstLetter = string.sub(word, 1, 1)
        local restOfString = string.sub(word, 2)
        return string.upper(firstLetter) .. string.lower(restOfString)
    end

    local words = {}
    for word in sentence:gmatch("%S+") do
        table.insert(words, capitalizeWord(word))
    end

    return table.concat(words, " ")
end
'
END;

select capitalize_words('hello, world');



CREATE FUNCTION create_counter()
    RETURNs int
    LANGUAGE LUA
BEGIN
'
function create_counter()
    local count = 0

    local function increment()
        count = count + 1
        return count
    end

    return increment
end
'
END;
select create_counter();


CREATE FUNCTION string_manipulator(input nchar)
    RETURNs int
    LANGUAGE LUA
BEGIN
'
function string_manipulator(inputString)
    local function countWords(inputString)
        local _, count = inputString:gsub("%S+", "")
        return count
    end
    return countWords(inputString)
end
'
END;
select string_manipulator('hello, world');

show functions;
drop function fibonacci1;
drop function reverse_string;
drop function count_character;
drop function is_prime;
drop function is_over_100_years;
drop function capitalize_words;
drop function create_counter;
drop function string_manipulator;

drop database test_complex_udf;
