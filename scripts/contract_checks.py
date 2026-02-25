import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load(rel):
    p = ROOT / rel
    txt = p.read_text(encoding='utf-8')
    return p, txt, ast.parse(txt)


def find_func(tree, name):
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name:
            return n
    return None


def find_class(tree, name):
    for n in tree.body:
        if isinstance(n, ast.ClassDef) and n.name == name:
            return n
    return None


def methods(cls):
    return {n.name: n for n in cls.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def args(fn):
    return [a.arg for a in fn.args.args] + [a.arg for a in fn.args.kwonlyargs]


def main() -> int:
    failures = []

    # pipeline checks
    p_path, p_txt, p_ast = load('engine/pipeline.py')
    runp = find_func(p_ast, 'run_pipeline')
    if not runp:
        failures.append('run_pipeline missing')
        rp_args = []
    else:
        rp_args = args(runp)
        for req in ['config', 'store']:
            if req not in rp_args:
                failures.append(f'run_pipeline missing required param {req}')
    if '.fetch(' in p_txt:
        failures.append('pipeline still calls ingester.fetch(); expected ingest()')

    # check all current run_pipeline callsites use supported kwargs/positions (heuristic)
    for rel in ['main.py', 'bot/telegram_commands.py']:
        _, txt, tree = load(rel)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'run_pipeline':
                for kw in node.keywords:
                    if kw.arg and kw.arg not in rp_args:
                        failures.append(f'{rel} calls run_pipeline with unsupported kwarg {kw.arg}')

    # telegram error handler exists and no obvious undefined logger symbol usage
    t_path, t_txt, t_ast = load('bot/telegram_commands.py')
    terr = find_func(t_ast, 'telegram_error_handler')
    if not terr:
        failures.append('telegram_error_handler missing')
    elif not isinstance(terr, ast.AsyncFunctionDef):
        failures.append('telegram_error_handler should be async')
    terr_src = ast.get_source_segment(t_txt, terr) if terr else ''
    module_names = {n.targets[0].id for n in t_ast.body if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name)}
    module_names |= {n.name for n in t_ast.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
    if terr_src and 'logger.' in terr_src and 'logger' not in module_names:
        failures.append('telegram_error_handler references undefined logger')

    # store contracts used by commands
    _, store_txt, store_ast = load('storage/sqlite_store.py')
    scls = find_class(store_ast, 'SQLiteStore')
    if not scls:
        failures.append('SQLiteStore missing')
    else:
        sm = methods(scls)
        for name in ['get_signals_since', 'set_meta', 'get_meta']:
            if name not in sm:
                failures.append(f'SQLiteStore missing {name}')
        if 'get_signals_since' in sm and 'limit' not in args(sm['get_signals_since']):
            failures.append('SQLiteStore.get_signals_since missing limit param')

    # ingester contracts
    specs = [
        ('ingestion/news_ingest.py', 'NewsIngester'),
        ('ingestion/funding_ingest.py', 'FundingIngester'),
        ('ingestion/ecosystem_ingest.py', 'EcosystemIngester'),
        ('ingestion/github_ingest.py', 'GitHubIngester'),
        ('ingestion/twitter_ingest.py', 'TwitterIngester'),
    ]
    for rel, cname in specs:
        _, _, tree = load(rel)
        cls = find_class(tree, cname)
        if not cls:
            failures.append(f'{cname} missing in {rel}')
            continue
        m = methods(cls)
        if 'ingest' not in m:
            failures.append(f'{cname} missing ingest()')

    if failures:
        print('CONTRACT_CHECK_FAIL')
        for f in failures:
            print('-', f)
        return 1
    print('CONTRACT_CHECK_PASS')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
