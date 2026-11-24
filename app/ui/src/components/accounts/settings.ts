import type { AccountSettings, Json, JsonObject } from "../../core";

export interface Field {
  name: string;
  title: string;
  schema: JsonObject;
  required: boolean;
  secret: boolean;
}

export type Edits = ReadonlyMap<string, string | null>;
export const emptySettings: AccountSettings = { values: {}, secretsSet: [] };

function object(value: Json | undefined): value is JsonObject {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function fieldsFor(schema: JsonObject | null): Field[] {
  if (!object(schema?.properties)) return [];
  const required = Array.isArray(schema.required) ? schema.required : [];
  return Object.entries(schema.properties).map(([name, value]) => {
    const property = object(value) ? value : {};
    return {
      name, schema: property, title: typeof property.title === "string" ? property.title : name,
      required: required.includes(name), secret: property["x-secret"] === true,
    };
  }).sort((a, b) => Number(a.schema["x-order"] ?? Infinity) - Number(b.schema["x-order"] ?? Infinity));
}

function original(field: Field, settings: AccountSettings) {
  return Object.hasOwn(settings.values, field.name) ? settings.values[field.name] : field.schema.default;
}

export function displayValue(field: Field, settings: AccountSettings, edits: Edits): string {
  if (edits.has(field.name)) return edits.get(field.name) ?? "";
  if (field.secret) return "";
  const value = original(field, settings);
  if (value === undefined) return "";
  if (Array.isArray(field.schema.enum)) {
    const index = field.schema.enum.findIndex(item => JSON.stringify(item) === JSON.stringify(value));
    return index < 0 ? "" : String(index);
  }
  if (field.schema.type === "string") return String(value);
  return JSON.stringify(value, null, 2);
}

export function settingsPayload(fields: Field[], settings: AccountSettings, edits: Edits): JsonObject {
  const values = new Map(Object.entries(settings.values));
  for (const name of settings.secretsSet) values.delete(name);
  for (const field of fields) {
    const { name, schema, title } = field;
    const raw = edits.get(name);
    if (field.secret) {
      values.delete(name);
      if (raw === null || raw) values.set(name, raw);
      if (field.required && !raw && (raw === null || !settings.secretsSet.includes(name))) {
        throw new Error(`${title} is required.`);
      }
      continue;
    }
    if (raw === undefined) {
      if (field.required && original(field, settings) === undefined) {
        if (schema.type === "boolean" && !Array.isArray(schema.enum)) values.set(name, false);
        else throw new Error(`${title} is required.`);
      }
      continue;
    }
    if (raw === null || (raw === "" && (schema.type !== "string" || Array.isArray(schema.enum)))) {
      if (field.required) throw new Error(`${title} is required.`);
      values.delete(name);
      continue;
    }
    let value: Json;
    if (Array.isArray(schema.enum)) {
      const index = Number(raw);
      if (!Number.isInteger(index) || index < 0 || index >= schema.enum.length) throw new Error(`Choose a value for ${title}.`);
      value = schema.enum[index];
    } else if (schema.type === "number" || schema.type === "integer") {
      value = Number(raw);
      if (!Number.isFinite(value) || (schema.type === "integer" && !Number.isSafeInteger(value))) {
        throw new Error(`Enter a valid ${schema.type} for ${title}.`);
      }
    } else if (schema.type === "boolean") {
      value = raw === "true";
    } else if (schema.type === "string") {
      value = raw;
    } else {
      try { value = JSON.parse(raw); } catch { throw new Error(`Enter valid JSON for ${title}.`); }
      if (schema.type === "object" && !object(value) || schema.type === "array" && !Array.isArray(value)) {
        throw new Error(`${title} must be a JSON ${schema.type}.`);
      }
    }
    values.set(name, value);
  }
  return Object.fromEntries(values);
}
