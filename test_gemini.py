"""
test_gemini.py - Gemini API 動作確認スクリプト

使い方:
  GEMINI_API_KEY=your_key python test_gemini.py
  python test_gemini.py                          # .env から自動読み込み
  python test_gemini.py --model gemini-2.5-flash@v1beta  # モデル指定
"""
import os, sys, argparse

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from llm_client import call_gemini, call_llm


def test_basic(api_key: str, model_spec: str | None = None) -> bool:
    """最小プロンプトで Gemini の疎通確認"""
    if model_spec:
        os.environ["GEMINI_MODEL_CANDIDATES"] = model_spec

    prompt = "「テスト成功」とだけ返してください。"
    print(f"[TEST] Gemini疎通確認 prompt='{prompt}'")
    try:
        result = call_gemini(api_key, prompt, temperature=0.0, max_tokens=32)
        print(f"[OK]   レスポンス: {result!r}")
        return True
    except Exception as e:
        print(f"[NG]   エラー: {e}")
        return False


def test_call_llm() -> bool:
    """call_llm() 経由での疎通確認（LLM_PROVIDER=gemini デフォルト）"""
    os.environ.setdefault("LLM_PROVIDER", "gemini")
    prompt = "「OK」とだけ返してください。"
    print(f"\n[TEST] call_llm() 経由 prompt='{prompt}'")
    try:
        result = call_llm(prompt)
        print(f"[OK]   レスポンス: {result!r}")
        return True
    except Exception as e:
        print(f"[NG]   エラー: {e}")
        return False


def test_tsv_stub(api_key: str) -> bool:
    """TSV フォーマット出力の簡易確認（3行だけ生成）"""
    prompt = (
        "次のTSV形式で3行だけ出力してください（ヘッダー不要）:\n"
        "id\tplatform\ttext\n"
        "例:\n"
        "1\tthreads\tテスト投稿文サンプル\n"
        "```tsv で囲まずプレーンテキストで出力してください。"
    )
    print("\n[TEST] TSV出力フォーマット確認")
    try:
        result = call_gemini(api_key, prompt, temperature=0.5, max_tokens=256)
        lines = [l for l in result.strip().splitlines() if "\t" in l]
        print(f"[OK]   TSVタブ区切り行数: {len(lines)}")
        for ln in lines[:3]:
            print(f"       {ln}")
        return len(lines) >= 1
    except Exception as e:
        print(f"[NG]   エラー: {e}")
        return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--model", default=None,
                   help="モデル指定例: gemini-2.5-flash@v1beta")
    p.add_argument("--skip-tsv", action="store_true",
                   help="TSVフォーマットテストをスキップ")
    args = p.parse_args()

    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        print("[ERROR] GEMINI_API_KEY が未設定です")
        print("        export GEMINI_API_KEY=AIza... を設定してから実行してください")
        sys.exit(1)

    print(f"[INFO] GEMINI_API_KEY 確認済み（末尾4文字: ...{api_key[-4:]}）")

    results = []
    results.append(test_basic(api_key, args.model))
    results.append(test_call_llm())
    if not args.skip_tsv:
        results.append(test_tsv_stub(api_key))

    print()
    if all(results):
        print("[PASS] すべてのテストが成功しました")
        sys.exit(0)
    else:
        failed = sum(1 for r in results if not r)
        print(f"[FAIL] {failed}/{len(results)} 件のテストが失敗しました")
        sys.exit(1)


if __name__ == "__main__":
    main()
