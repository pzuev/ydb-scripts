#!/usr/bin/env python3

import sys
import argparse
import json

COMPLEX_ARGS = {
    'KqpPhysicalQuery',
    'KqpBlockReadOlapTableRanges',
    'KqpPhysicalTx',
    'KqpTxResultBinding',
    # 'DqPhyStage',
    'DqPhyHashCombine',
    'WideCombiner',
    'BlockHashJoinCore',
    'BlockAsStruct',
    'BlockMergeFinalizeHashed',
    'BlockCombineHashed',
    'TopSort',
    'NarrowMap',
    'WideMap',
    'WideFilter',
    'ExpandMap',
    'Condense',
    'WideCondense',
    'Condense1',
    'WideCondense1',
    'KqpOlapFilter',
    'KqpOlapAnd',
    'StructType',
    'AsStruct',
    'Udf',
    'Apply',
}

class List:
    def __init__(self, is_quote):
        self.list = []
        self.is_quote = is_quote

class Element:
    def __init__(self, is_quote, value, is_quoted_str=False):
        self.value = value
        self.is_quote = is_quote
        self.is_quoted_str = is_quoted_str

class Reference:
    def __init__(self, alias):
        self.alias = alias

def read_string(line, pos):
    esc = False
    res = ''
    while pos < len(line):
        if esc:
            res += line[pos]
            pos += 1
            esc = False
            continue
        if line[pos] == '\\':
            esc = True
            pos += 1
            continue
        if line[pos] == '"':
            return res, pos + 1
        res += line[pos]
        pos += 1
    raise Exception("unterminated string")

def read_num(line, pos):
    start = pos
    while pos < len(line):
        if not line[pos].isdigit():
            return int(line[start:pos]), pos
        pos += 1
    return int(line[start:]), pos

def read_keyword(line, pos):
    start = pos
    while pos < len(line):
        if line[pos] == ')' or line[pos].isspace():
            return line[start:pos], pos
        pos += 1
    raise Exception("unterminated keyword: ", line[start:])

def get_oper_from_raw_list(the_list):
    oper = None
    if len(the_list) >= 1 and isinstance(the_list[0], Element):
        item = the_list[0]
        if not item.is_quote and not item.is_quoted_str and isinstance(item.value, str):
            oper = item.value
    return oper

def get_oper(the_list):
    return get_oper_from_raw_list(the_list.list)

def print_list(out, the_list, callables, shift=0):
    def print_shift(sh):
        out.write(" "*(sh*4))

    oper = get_oper(the_list)
    is_long_oper = oper is not None and (oper in COMPLEX_ARGS)
    if is_long_oper and len(the_list.list) <= 2:
        is_long_oper = False
    is_block_oper = oper is not None and (oper in ('block'))

    if is_long_oper:
        shift += 1

    child_list = {}
    if oper and oper in callables:
        child_list = callables[oper].children_names

    for pos, item in enumerate(the_list.list):
        is_last = (pos == (len(the_list.list) - 1))
        is_first = (pos == 0)

        if not is_first and is_long_oper:
            out.write('\n')
            print_shift(shift)

        if pos > 0:
            param_name = child_list.get(pos - 1, None)
            if param_name and param_name != 'Input' and oper != 'Apply':  # hack for less visual noise
                out.write(param_name)
                out.write(': ')

        if isinstance(item, List):
            arg_shift = shift
            if item.is_quote:
                out.write('\'')
            out.write('(')
            if is_block_oper:
                arg_shift += 1
                out.write('\n')
                print_shift(arg_shift)
            sub_oper = print_list(out, item, callables, arg_shift)
            out.write(')')
            if sub_oper in ('return', 'let'):
                out.write('\n')
                if is_last:
                    print_shift(shift-1)
                else:
                    print_shift(shift)
            elif not is_last:
                out.write(' ')
        elif isinstance(item, Element):
            if item.is_quote:
                out.write('\'')
            if item.is_quoted_str:
                out.write('"')
                out.write(item.value.encode('unicode_escape').decode('utf-8'))
                out.write('"')
            else:
                out.write(str(item.value))
            if not is_last:
                out.write(' ')
        elif isinstance(item, Reference):
            out.write('$')
            out.write(str(item.alias))
            if not is_last:
                out.write(' ')
        else:
            raise Exception("Unknown list element type:", item.__class__.__name__)

    if is_long_oper:
        shift -= 1
        out.write('\n')
        print_shift(shift)

    return oper


class Macro:
    def __init__(self, definition, is_leaf):
        self.definition = definition
        self.is_leaf = is_leaf

def collect_refs(the_list):
    table = {}
    ref_counts = {}
    tail = None
    is_leaf = True
    scanning_ref_id = None

    if len(the_list.list) > 2:
        oper = None
        item = the_list.list[0]
        if isinstance(item, Element) and not item.is_quote and not item.is_quoted_str and isinstance(item.value, str):
            oper = item.value
        ref = the_list.list[1]
        if oper =='let' and isinstance(ref, Reference):
            scanning_ref_id = ref.alias
            tail = the_list.list[2:]

    if tail is None:
        tail = the_list.list

    for item in tail:
        if isinstance(item, List):
            sub_table, sub_counts, sub_is_leaf = collect_refs(item)
            if not sub_is_leaf:
                is_leaf = False
            table.update(sub_table)
            for ref, cnt in sub_counts.items():
                ref_counts[ref] = ref_counts.get(ref, 0) + cnt
        elif isinstance(item, Reference):
            is_leaf = False
            ref = item.alias
            ref_counts[ref] = ref_counts.get(ref, 0) + 1

    if scanning_ref_id is not None:
        table[scanning_ref_id] = Macro(tail, is_leaf)

    return table, ref_counts, is_leaf

def simple_enough_macro(the_list):
    simple = True
    for item in the_list:
        if isinstance(item, List):
            oper = get_oper(item)
            if oper is None:
                if not simple_enough_macro(item.list):
                    simple = False
                    break
                continue
            simple = simple and (oper in ('StructType', 'DataType', 'ResourceType', 'TupleType', 'CallableType', 'VoidType', 'Void', 'BlockType'))
    return simple

def replace_refs(the_list, table, ref_counts, current_let_ref_id=None):
    rebuilt = []
    did_replace = set()

    lets = []

    for pos, item in enumerate(the_list):
        if isinstance(item, List):
            sub_list = item.list
            ref_id = None
            if len(sub_list) > 2:
                if isinstance(sub_list[0], Element) and not sub_list[0].is_quote and not sub_list[0].is_quoted_str and sub_list[0].value == 'let' and isinstance(sub_list[1], Reference):
                    ref_id = sub_list[1].alias
                    # Remove let definitions that are guaranteed to be replaced
                    if not (ref_counts.get(ref_id, 0) == 1 or table[ref_id].is_leaf):
                        l = List(item.is_quote)
                        l.list, sub_did_replace = replace_refs(sub_list, table, ref_counts, ref_id)
                        lets.append((ref_id, l, sub_did_replace))
                    continue
            l = List(item.is_quote)
            l.list, sub_did_replace = replace_refs(sub_list, table, ref_counts, None)
            did_replace |= sub_did_replace
            rebuilt.append(l)
            continue

        if isinstance(item, Reference):
            ref_id = item.alias
            if ref_id == current_let_ref_id:
                rebuilt.append(item)
                continue

            if ref_id in table:
                should_replace = False
                if (ref_counts.get(ref_id, 0) == 1 or table[ref_id].is_leaf):
                    should_replace = True

                # this will copy referenced list before mutating
                replaced, sub_did_replace = replace_refs(table[ref_id].definition, table, ref_counts)

                # if not should_replace:
                #     oper = get_oper_from_raw_list(the_list)
                #     if oper == 'DqPhyStage' and pos == 2:
                #         should_replace = True

                if not should_replace:
                    # Maybe we still can decide to replace if the content is simple enough
                    should_replace = simple_enough_macro(replaced)

                if should_replace:
                    rebuilt += replaced
                    did_replace.add(ref_id)
                    did_replace |= sub_did_replace
                else:
                    rebuilt.append(item)
                continue

        rebuilt.append(item)

    filtered_lets = []
    lets.reverse()
    for let_id, let_content, let_replace_set in lets:
        if let_id not in did_replace:
            filtered_lets.append(let_content)
            did_replace |= let_replace_set
    filtered_lets.reverse()

    return filtered_lets + rebuilt, did_replace

def parse(f):
    curr_stack = [List(False)]
    is_quote = False

    def push(item):
        curr_stack[-1].list.append(item)

    for line in f:
        line = line.strip()
        if not line:
            continue
        pos = 0
        while pos < len(line):
            if line[pos] == '\'':
                is_quote = True
                pos += 1
                continue

            if line[pos] == '(':
                l = List(is_quote)
                push(l)
                curr_stack.append(l)
                pos += 1
            elif line[pos] == '"':
                tok, pos = read_string(line, pos + 1)
                push(Element(is_quote, tok, is_quoted_str=True))
            elif line[pos].isdigit():
                tok, pos = read_num(line, pos)
                push(Element(is_quote, tok))
            elif line[pos] == ')':
                curr_stack.pop()
                pos += 1
            elif line[pos] == '$':
                tok, pos = read_num(line, pos+1)
                push(Reference(tok))
            elif line[pos].isspace():
                pos += 1
            else:
                tok, pos = read_keyword(line, pos)
                push(Element(is_quote, tok))
            is_quote = False

    return curr_stack[0]

class NodeDescr:
    def __init__(self, name, base, match_callable, children_names):
        self.name = name
        self.base = base
        self.children_names = children_names
        self.match_callable = match_callable

def parse_node_file(node_file):
    result = {}
    js = json.loads(node_file.read().strip())
    for node in js.get('Nodes', []):
        name = node.get('Name', None)
        if not name:
            continue
        base = node.get('Base', None)
        match = node.get('Match', {})
        match_type = match.get('Type', None)
        match_callable = None
        if match_type == 'Callable':
            match_callable = match.get('Name', None)
        child_names = {}
        for child in node.get('Children', []):
            child_index = int(child.get('Index', -1))
            child_name = child.get('Name', None)
            if not child_name:
                child_name = None
            if child_name:
                child_names[child_index] = child_name
        result[name] = NodeDescr(name, base, match_callable, child_names)

    return result

def inherit_children(node_descriptions):
    for node in node_descriptions.values():
        children_names = dict(node.children_names)
        parent_node = node
        while parent_node.base:
            parent_node = node_descriptions.get(parent_node.base, None)
            if parent_node is None:
                break
            new_children_names = dict(parent_node.children_names)
            new_children_names.update(children_names)
            children_names = new_children_names
        node.children_names = children_names

def add_hardcoded(node_descriptions):
    def try_add(callable, children):
        if callable in node_descriptions:
            return
        node_descriptions[callable] = NodeDescr(callable, None, callable, children)

    try_add('WideTakeBlocks', {0: 'Input', 1: 'Count'})

def build_callable_index(node_descriptions):
    result = {}
    for node in node_descriptions.values():
        if not node.match_callable:
            continue
        if len(node.children_names) == 1 and node.children_names.get(0, None) in ('Literal', 'Type', 'ItemType', 'OptionalType'):
            continue
        if len(node.children_names) == 2 and node.children_names.get(0, None) == 'Left':
            continue
        result[node.match_callable] = node

    def add_alias(alias, original):
        if alias not in result and original in result:
            result[alias] = result[original]

    add_alias('WideCondense1', 'Condense1')

    return result

if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-n', '--nodes', default=[], action='append')
    args = argparser.parse_args()

    node_descrs = {}
    for node_file in args.nodes:
        with open(node_file, 'rt') as inf:
            node_descrs.update(parse_node_file(inf))

    print('Loaded %d nodes' % len(node_descrs), file=sys.stderr)
    add_hardcoded(node_descrs)
    inherit_children(node_descrs)
    callables = build_callable_index(node_descrs)
    print('%d callables' % len(callables), file=sys.stderr)

    program = parse(sys.stdin)
    ref_table, ref_counts, _ = collect_refs(program)
    replaced_program = List(False)
    replaced_program.list, _ = replace_refs(program.list, ref_table, ref_counts)
    print_list(sys.stdout, replaced_program, callables)
