import { buttonVariants } from "fumadocs-ui/components/ui/button";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "fumadocs-ui/components/ui/collapsible";
import { ChevronRight } from "lucide-react";
import type { ReactNode } from "react";

// fumadocs-python 0.1.1 ships its components as one bundle that lost the
// "use client" directive of its Base UI collapsible, so its PySourceCode
// passes a function-valued className across the server/client boundary and
// crashes static prerendering. Mirror upstream's PySourceCode on top of
// fumadocs-ui's client-marked Radix collapsible instead; drop this override
// once the upstream bundle carries the directive again.
export function PySourceCode({ children }: { children: ReactNode }) {
  return (
    <Collapsible className="my-6">
      <CollapsibleTrigger
        className={buttonVariants({
          color: "secondary",
          size: "sm",
          className: "group",
        })}
      >
        Source Code
        <ChevronRight className="size-3.5 text-fd-muted-foreground group-data-[state=open]:rotate-90" />
      </CollapsibleTrigger>
      <CollapsibleContent className="prose-no-margin">
        {children}
      </CollapsibleContent>
    </Collapsible>
  );
}
