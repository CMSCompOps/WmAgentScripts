
import re
import classad

_split_re = re.compile(",\s*")
def sortStringSet(in_list, state={}):
    if isinstance(in_list, classad.ExprTree):
        in_list = in_list.eval(state)
    if isinstance(in_list, classad.Value):
        return classad.Value.Undefined
    split_list = _split_re.split(in_list)
    split_list = list(set(split_list))
    split_list.sort()
    return ",".join(split_list)

classad.register(sortStringSet)

