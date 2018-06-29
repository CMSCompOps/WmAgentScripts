
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

_split_re = re.compile(",\s*")
def siteMapping(in_list, source_to_dests, state={}):
    if isinstance(in_list, classad.ExprTree):
        in_list = in_list.eval(state)
    if isinstance(in_list, classad.Value):
        return classad.Value.Undefined

    split_list = _split_re.split(in_list)
    final_set = set()
    for site in split_list:
        ## add the source sites
        final_set.add(site)
        ## add all destination sites
        final_set.update(source_to_dests.setdefault(site, set()))
    split_list = list(final_set)
    split_list.sort()
    return str(",".join(split_list))

def removeSite(those,from_sites):
    to_remove = set(_split_re.split(those))
    from_sites = set(_split_re.split(from_sites))
    return str(",".join( sorted( from_sites - to_remove )))

classad.register(sortStringSet)
classad.register(siteMapping)
classad.register(removeSite)
