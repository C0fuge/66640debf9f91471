import { useId } from "react";
import type { AccountSettings } from "../../core";
import { displayValue, type Edits, type Field } from "./settings";

interface Props {
  field: Field;
  settings: AccountSettings;
  edits: Edits;
  edit: (name: string, value: string | null) => void;
}

export function SettingsField({ field, settings, edits, edit }: Props) {
  const id = useId();
  const { name, schema, required, secret } = field;
  const value = displayValue(field, settings, edits);
  const set = (next: string | null) => edit(name, next);
  const configured = secret && settings.secretsSet.includes(name);
  const clearing = configured && edits.get(name) === null;
  const common = { id, required, "aria-describedby": typeof schema.description === "string" ? id + "-hint" : undefined };
  const lengths = {
    minLength: typeof schema.minLength === "number" ? schema.minLength : undefined,
    maxLength: typeof schema.maxLength === "number" ? schema.maxLength : undefined,
  };
  let control;
  if (secret) {
    control = <><input {...common} {...lengths} type="password" autoComplete="new-password"
      required={required && !configured} value={value} disabled={clearing}
      placeholder={configured ? "Configured" : undefined} onChange={e => set(e.target.value)} />
      {configured && !required && <label className="account-check"><input type="checkbox" checked={clearing}
        onChange={e => set(e.target.checked ? null : "")} />Clear stored value</label>}</>;
  } else if (Array.isArray(schema.enum)) {
    control = <select {...common} value={value} onChange={e => set(e.target.value)}>
      <option value="">Select a value</option>
      {schema.enum.map((option, i) => <option key={i} value={i}>{typeof option === "string" ? option : JSON.stringify(option)}</option>)}
    </select>;
  } else if (schema.type === "boolean") {
    control = <input id={id} aria-describedby={common["aria-describedby"]} type="checkbox" checked={value === "true"}
      onChange={e => set(String(e.target.checked))} />;
  } else if (schema.type === "integer" || schema.type === "number") {
    control = <input {...common} type="number" value={value} step={schema.type === "integer" ? 1 : "any"}
      min={typeof schema.minimum === "number" ? schema.minimum : undefined}
      max={typeof schema.maximum === "number" ? schema.maximum : undefined} onChange={e => set(e.target.value)} />;
  } else if (schema.type === "string" && schema["x-widget"] !== "textarea") {
    control = <input {...common} {...lengths} type="text" value={value} onChange={e => set(e.target.value)} />;
  } else {
    control = <textarea {...common} {...lengths} value={value} rows={schema.type === "string" ? 2 : 4}
      spellCheck={false} onChange={e => set(e.target.value)} />;
  }
  return <div className="account-field" data-checkbox={schema.type === "boolean" && !secret && !Array.isArray(schema.enum)}>
    <label htmlFor={id}>{field.title}{required && <span aria-hidden="true"> *</span>}</label>
    {control}
    {typeof schema.description === "string" && <small id={id + "-hint"}>{schema.description}</small>}
  </div>;
}
