def test_concise_round():
    from nodestats import concise_round

    testdata = [
        (48.01, 48  ),
        (17.16, 17  ),
        ( 10.5, 10  ),
        ( 9.08, 9   ),
        ( 7.72, 8   ),
        ( 5.98, 6   ),
        ( 5.28, 5   ),
        ( 4.88, 5   ),
        ( 4.25, 4   ),
        ( 2.42, 2.4 ),
        ( 2.17, 2   ),
        ( 2.03, 2   ),
        ( 1.9 , 2   ),
        ( 1.8 , 1.8 ),
        ( 1.45, 1.4 ),
        ( 1.42, 1.4 ),
        ( 1.41, 1.4 ),
        ( 0.9 , 0.9 ),
        ( 0.46, 0.5 ),
        ( 0.36, 0.36),
    ]

    for inp, exp in testdata:
        res = concise_round(inp)
        st = "OK" if exp == res else "FAIL"
        print("{:6} -> {:4}  {}".format(inp, res, st))

test_concise_round()
