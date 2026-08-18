# Integrate exp(-x) over [a, b] by the trapezoid rule.
#
# The tool this example runs at a placement. Deliberately not Python: the
# point of `shell(...)` is that the work is an external command, and awk is
# the smallest real one that is everywhere a farm node is.
#
# Reads a grid declaration as `name=value` lines and writes one number to the
# file named by `-v out=...`, because a declared output is a path the executor
# will look at afterwards, not something scraped from a stream.

BEGIN { FS = "=" }

$1 == "a"     { a = $2 + 0 }
$1 == "b"     { b = $2 + 0 }
$1 == "steps" { n = $2 + 0 }

END {
    if (n <= 0) { print "steps must be positive" > "/dev/stderr"; exit 1 }
    if (out == "") { print "no output file given" > "/dev/stderr"; exit 1 }
    h = (b - a) / n
    total = (exp(-a) + exp(-b)) / 2
    for (i = 1; i < n; i++) total += exp(-(a + i * h))
    printf "%.12f\n", total * h > out
}
