from collections import defaultdict

def _parse_money(x):
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    # remove common formatting
    s = s.replace("$", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return None

def _find_value_by_keywords(record, keywords):
    """
    Finds a value in a dict where the key matches any keyword
    (exact or partial, case-insensitive).
    """
    lower_keys = {k.lower(): k for k in record.keys()}

    # exact match first
    for kw in keywords:
        if kw.lower() in lower_keys:
            return record[lower_keys[kw.lower()]]

    # partial match next
    for k in record.keys():
        kl = k.lower()
        for kw in keywords:
            if kw.lower() in kl:
                return record[k]
    return None

def get_top_sales_reps(records, top_n=5):
    revenue_by_rep = defaultdict(float)

    for record in records:
        # 1) Try canonical keys first
        rep = record.get("sales_rep")
        amount = record.get("deal_value")

        # 2) If canonical keys not present, fallback to keyword detection
        if not rep:
            rep = _find_value_by_keywords(record, ["rep", "sales_rep", "agent", "salesperson", "owner"])
        if amount is None or amount == "":
            amount = _find_value_by_keywords(record, ["amount", "value", "revenue", "price", "total"])

        if not rep:
            continue

        parsed = _parse_money(amount)
        if parsed is None:
            continue

        revenue_by_rep[str(rep).strip()] += parsed

    ranked = sorted(revenue_by_rep.items(), key=lambda x: x[1], reverse=True)

    return [
        {"sales_rep": rep, "total_revenue": round(total, 2)}
        for rep, total in ranked[:top_n]
    ]
