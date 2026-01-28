#!/usr/bin/env python3

import sys
import argparse
import json

COMPLEX_ARGS = {
    'DqCnHashShuffle',
    'DqCnMerge',
    'DqReplicate',
    'KqpPhysicalQuery',
    'KqpBlockReadOlapTableRanges',
    'KqpPhysicalTx',
    'KqpTxResultBinding',
    'DqPhyStage',
    'DqPhyHashCombine',
    'WideCombiner',
    'BlockHashJoinCore',
    'BlockAsStruct',
    'BlockMergeFinalizeHashed',
    'BlockCombineHashed',
    'TopSort',
    'Map',
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
    'List',
    'AsList',
    'RangeCreate',
    'RangeFinalize',
    'RangeMultiply',
    'RangeIntersect',
    'RangeUnion',
    'If',
    'IfPresent',
    'TupleType',
}

SIMPLE_OPERATORS = {
    'OptionalType',
    'StructType',
    'DataType',
    'ResourceType',
    'TupleType',
    'ListType',
    'CallableType',
    'VoidType',
    'Void',
    'BlockType',
    'Nothing',
    'SafeCast',
    'String',
    '-',
    '+',
    '*',
    '/',
    'Int32',  # TODO: generate a collection of types
}

COLOR_COMMENT = 'comment'
COLOR_FUNC_NAME = 'func'
COLOR_SPECIAL_EXPR = 'spec'
COLOR_STRING_LITERAL = 'str'
COLOR_LITERAL = 'literal'
COLOR_ARG = 'arg'
COLOR_LAMBDA = COLOR_ARG
COLOR_REF = None
COLOR_TABLINE = 'tabline'

COLORS = {
    COLOR_COMMENT: '2;128;128;128',
    COLOR_FUNC_NAME: '2;0;128;128',
    COLOR_SPECIAL_EXPR: '2;128;0;128',
    COLOR_STRING_LITERAL: '2;64;192;192',
    COLOR_LITERAL: '2;64;192;192',
    COLOR_ARG: '2;192;156;0',
    COLOR_TABLINE: '2;64;64;64'
}


class Color:
    def __init__(self, color_name):
        if color_name and (color_name in COLORS) and sys.stdout.isatty():
            self.color = COLORS[color_name]
        else:
            self.color = None

    def __enter__(self):
        if self.color:
            sys.stdout.write('\033[38;%sm' % self.color)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.color:
            sys.stdout.write('\033[39m')


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


def get_oper_from_raw_list(the_list):
    oper = None
    if len(the_list) >= 1 and isinstance(the_list[0], Element):
        item = the_list[0]
        if not item.is_quote and not item.is_quoted_str and isinstance(item.value, str):
            oper = item.value
    return oper


def get_oper(the_list):
    return get_oper_from_raw_list(the_list.list)


def get_oper_color(oper):
    if not oper:
        return None
    elif oper == 'lambda':
        return COLOR_LAMBDA
    elif oper in ('block', 'let', 'return', 'declare'):
        return COLOR_SPECIAL_EXPR
    else:
        return COLOR_FUNC_NAME


class Context:
    def __init__(self, parent=None, shift=None, is_lambda_args=False, tabstops=None):
        self.shift = 0
        self.lambda_args = set()
        if parent is not None:
            self.tabstops = parent.tabstops
            self.shift = parent.shift
            if not is_lambda_args:
                self.lambda_args.update(parent.lambda_args)
        if shift is not None:
            self.shift = shift
        if tabstops is not None:
            self.tabstops = tabstops
        self.is_lambda_args = is_lambda_args


def get_is_long_oper(the_list: List):
    if len(the_list.list) <= 2:
        return False
    oper = get_oper(the_list)
    return oper is not None and (oper in COMPLEX_ARGS)


def has_long_or_block_oper_inside(item):
    if isinstance(item, List):
        if get_is_long_oper(item) or get_oper(item) == 'block':
            return True
        for sub_item in item.list:
            if has_long_or_block_oper_inside(sub_item):
                return True
    else:
        return False


def print_list(out, the_list: List, callables, context: Context):
    def print_shift(sh):
        for _ in range(sh):
            if context.tabstops:
                with Color(COLOR_TABLINE):
                    out.write('\u2506   ')
            else:
                out.write('    ')

    oper = get_oper(the_list)
    is_long_oper = get_is_long_oper(the_list)
    is_block_oper = oper is not None and (oper in ('block'))

    if is_long_oper:
        context.shift += 1

    child_list = {}
    if oper and oper in callables:
        child_list = callables[oper].children_names

    for pos, item in enumerate(the_list.list):
        is_last = (pos == (len(the_list.list) - 1))
        is_first = (pos == 0)

        if not is_first and is_long_oper:
            out.write('\n')
            print_shift(context.shift)

        if pos > 0:
            param_name = child_list.get(pos - 1, None)
            if param_name == 'Input':
                param_name = '⇐'
            elif param_name == 'Lambda':
                param_name = 'λ'
            if param_name:
                with Color(COLOR_COMMENT):
                    out.write('⦗')
                    out.write(param_name)
                    out.write('⦘')
                if not is_first and is_long_oper and isinstance(item, List) and has_long_or_block_oper_inside(item):
                    out.write('\n')
                    print_shift(context.shift)

        if isinstance(item, List):
            is_lambda_args = (oper == 'lambda') and (pos == 1)
            sub_oper = get_oper(item)
            sub_oper_color = get_oper_color('block') if oper == 'block' else \
                get_oper_color(sub_oper) if not is_lambda_args else COLOR_ARG

            arg_shift = context.shift
            with Color(sub_oper_color):
                if item.is_quote:
                    out.write('\'')
                out.write('(')
            if is_block_oper:
                arg_shift += 1
                out.write('\n')
                print_shift(arg_shift)

            sub_ctx = Context(parent=context, shift=arg_shift, is_lambda_args=is_lambda_args)
            print_list(out, item, callables, sub_ctx)
            if is_lambda_args:
                context.lambda_args.update(sub_ctx.lambda_args)
            with Color(sub_oper_color):
                out.write(')')
            if sub_oper in ('return', 'let', 'declare'):
                out.write('\n')
                if is_last:
                    print_shift(context.shift-1)
                else:
                    print_shift(context.shift)
            elif not is_last:
                out.write(' ')
        elif isinstance(item, Element):
            if item.is_quote:
                with Color(COLOR_LITERAL):
                    out.write('\'')
            if item.is_quoted_str:
                with Color(COLOR_STRING_LITERAL):
                    out.write('"')
                    out.write(item.value.encode('unicode_escape').decode('utf-8'))
                    out.write('"')
            else:
                color = get_oper_color(oper) if (oper and pos == 0) else COLOR_LITERAL
                with Color(color):
                    out.write(str(item.value))
            if not is_last:
                out.write(' ')
        elif isinstance(item, Reference):
            if context.is_lambda_args:
                color = COLOR_ARG
                context.lambda_args.add(item.alias)
            else:
                color = COLOR_ARG if (item.alias in context.lambda_args) else COLOR_REF
            with Color(color):
                out.write('$')
                out.write(str(item.alias))

            if not is_last:
                out.write(' ')
        else:
            raise Exception("Unknown list element type:", item.__class__.__name__)

    if is_long_oper:
        context.shift -= 1
        out.write('\n')
        print_shift(context.shift)

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
            if oper == 'lambda' and len(item.list) > 1 and isinstance(item.list[1], List):
                lambda_args = set()
                for sub_item in item.list[1].list:
                    if isinstance(sub_item, Reference):
                        lambda_args.add(sub_item.alias)
                is_simple_lambda = False
                for def_item in item.list[2:]:
                    if not isinstance(def_item, Reference):
                        break
                    if def_item.alias not in lambda_args:
                        break
                else:
                    is_simple_lambda = True
                simple = simple and is_simple_lambda
                continue
            if oper is None:
                if not simple_enough_macro(item.list):
                    simple = False
                    break
                continue
            simple = simple and (oper in SIMPLE_OPERATORS)
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

                if not should_replace and ref_counts.get(ref_id) <= 3:
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


def simplify_blocks(the_list):
    """
    Replace (block '( (return a b c) ) with a b c.
    Returns a copy of the program, does not mutate anything in-place
    """
    result = []

    for item in the_list:
        if isinstance(item, List):
            if get_oper(item) == 'block' and not item.is_quote and len(item.list) == 2:
                block_content = item.list[1]
                if isinstance(block_content, List) and len(block_content.list) == 1:
                    maybe_return = block_content.list[0]
                    if get_oper(maybe_return) == 'return':
                        result += simplify_blocks(maybe_return.list[1:])
                        continue
            new_list = List(item.is_quote)
            new_list.list = simplify_blocks(item.list)
            result.append(new_list)
        else:
            result.append(item)

    return result


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
    raise Exception("unterminated quoted string")


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
    return line[start:]


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
        if len(node.children_names) == 1 and node.children_names.get(0, None) in ('Literal', 'Type', 'ItemType', 'OptionalType', 'Input', 'Apply', 'Callable'):
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
    argparser.add_argument('-r', '--repo', default=None)
    argparser.add_argument('-t', '--tabstops', action='store_true', default=False)
    args = argparser.parse_args()

    tabstops = args.tabstops

    node_descrs = {}
    node_files = []
    if args.repo:
        import os.path
        node_files += [os.path.join(args.repo, path) for path in [
            'ydb/library/yql/dq/expr_nodes/dq_expr_nodes.json',
            'ydb/core/kqp/expr_nodes/kqp_expr_nodes.json',
            'yql/essentials/core/expr_nodes/yql_expr_nodes.json',
        ]]
    node_files += args.nodes
    for node_file in node_files:
        with open(node_file, 'rt') as inf:
            node_descrs.update(parse_node_file(inf))

    # print('Loaded %d nodes' % len(node_descrs), file=sys.stderr)
    add_hardcoded(node_descrs)
    inherit_children(node_descrs)
    callables = build_callable_index(node_descrs)
    # print('%d callables' % len(callables), file=sys.stderr)

    program = parse(sys.stdin)
    ref_table, ref_counts, _ = collect_refs(program)
    replaced_program = List(False)
    replaced_program.list, _ = replace_refs(program.list, ref_table, ref_counts)
    simplified_program = List(False)
    simplified_program.list = simplify_blocks(replaced_program.list)
    print_list(sys.stdout, simplified_program, callables, Context(tabstops=tabstops))
    print()
