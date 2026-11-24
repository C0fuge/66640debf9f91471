import { useLayoutEffect, useRef, type ReactNode } from "react";
import { X } from "lucide-react";

export function AccountDialog({ title, subtitle, close, children }: {
  title: string; subtitle?: string; close: () => void; children: ReactNode;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  useLayoutEffect(() => {
    const dialog = ref.current!;
    dialog.showModal();
    return () => dialog.close();
  }, []);
  return <dialog ref={ref} className="account-dialog" aria-labelledby="account-dialog-title"
    onCancel={event => { event.preventDefault(); close(); }}>
    <header><div><h2 id="account-dialog-title">{title}</h2>{subtitle && <p>{subtitle}</p>}</div>
      <button type="button" className="account-close" aria-label="Close dialog" onClick={close}><X size={19} /></button>
    </header>
    <div className="account-dialog-body">{children}</div>
  </dialog>;
}
