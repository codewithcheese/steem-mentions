import math


def reputation_score(reputation):
    neg = False if reputation >= 0 else True
    out = math.log(math.fabs(reputation), 10)
    out = max(out - 9, 0)
    out = out * -1 if neg else out
    out = (out * 9) + 25
    return out


print(reputation_score(-463779708716))