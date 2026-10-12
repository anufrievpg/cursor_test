#!/usr/bin/env node
/**
 * Read-only snapshot: ЖК (complexes) → дома (houses).
 * Usage: MACROCRM_TOKEN='macro-3-…' node scripts/sync-macrocrm-estate.mjs
 */
import { writeFile, mkdir } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const BASE = process.env.MACROCRM_API_BASE ?? "https://macrocrm-api.enco.ru/v2";
const TOKEN = process.env.MACROCRM_TOKEN;

if (!TOKEN) {
  console.error("Set MACROCRM_TOKEN (Bearer token, read-only API key).");
  process.exit(1);
}

async function postList(path, body = {}) {
  const items = [];
  let from = body.from ?? null;
  for (let page = 0; page < 200; page++) {
    const payload = { ...body };
    if (from != null) payload.from = from;
    else delete payload.from;

    const res = await fetch(`${BASE}/${path}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${TOKEN}`,
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });
    const text = await res.text();
    if (!res.ok) {
      throw new Error(`${path} HTTP ${res.status}: ${text.slice(0, 300)}`);
    }
    const json = JSON.parse(text);
    const chunk = json.data ?? [];
    items.push(...chunk);
    const next = json.meta?.next;
    if (next == null || next === "") break;
    from = next;
  }
  return items;
}

/** Hierarchy: ЖК → дом */
function buildTree(complexes, houses) {
  const byId = new Map(complexes.map((c) => [c.id, { ...c, houses: [] }]));
  const orphan = { id: "__orphan__", publicName: "Дома без ЖК", houses: [] };

  for (const house of houses) {
    const parent = byId.get(house.complexId);
    if (parent) parent.houses.push(house);
    else orphan.houses.push(house);
  }

  const sortRu = (a, b) => String(a).localeCompare(String(b), "ru");
  const tree = [...byId.values()].sort((a, b) =>
    sortRu(a.publicName || a.id, b.publicName || b.id)
  );
  for (const cx of tree) {
    cx.houses.sort((a, b) =>
      sortRu(a.name || a.privateName || a.id, b.name || b.privateName || b.id)
    );
  }
  if (orphan.houses.length) {
    orphan.houses.sort((a, b) =>
      sortRu(a.name || a.privateName || a.id, b.name || b.privateName || b.id)
    );
    tree.push(orphan);
  }
  return tree;
}

const complexes = await postList("estateComplexes/list");
const houses = await postList("estateHouses/list");

const out = {
  fetchedAt: new Date().toISOString(),
  source: BASE,
  hierarchy: "complex → house",
  counts: { complexes: complexes.length, houses: houses.length },
  complexes,
  houses,
  tree: buildTree(complexes, houses),
};

const root = dirname(fileURLToPath(import.meta.url));
const outPath = join(root, "..", "docs", "data", "macrocrm-estate.json");
await mkdir(dirname(outPath), { recursive: true });
await writeFile(outPath, JSON.stringify(out, null, 2), "utf8");
console.log("Wrote", outPath, out.counts);
