import ast
from pathlib import Path

def summarize_directory(root_dir: str = ".") -> str:
    lines = ["# PySpark Project Documentation\n"]
    for py_file in sorted(Path(root_dir).rglob("*.py")):
        if "venv" in py_file.parts or ".git" in py_file.parts:
            continue
        source = py_file.read_text(encoding="utf-8", errors="ignore")
        tree = ast.parse(source)
        module_doc = ast.get_docstring(tree) or "No module description."

        lines.append(f"## `{py_file}`")
        lines.append(f"{module_doc}\n")

        funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef)]
        if funcs:
            lines.append("### Functions")
            for fn in funcs:
                args = [a.arg for a in fn.args.args]
                fn_doc = (ast.get_docstring(fn) or "No docstring.").splitlines()[0]
                lines.append(f"- **`{fn.name}({', '.join(args)})`**: {fn_doc}")
            lines.append("")
    return "\n".join(lines)

if __name__ == "__main__":
    Path("PROJECT_DOCS.md").write_text(summarize_directory("."), encoding="utf-8")
    print("Generated PROJECT_DOCS.md")
