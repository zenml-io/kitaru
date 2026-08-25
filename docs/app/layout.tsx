import type { Metadata } from "next";
import { DM_Sans } from "next/font/google";
import { Provider } from "@/components/provider";
import "./global.css";

const dmSans = DM_Sans({
  subsets: ["latin"],
  variable: "--font-dm-sans",
  display: "swap",
});

export const metadata: Metadata = {
  metadataBase: new URL("https://sdkdocs.kitaru.ai"),
  title: {
    default: "Kitaru SDK & CLI Reference",
    template: "%s | Kitaru Reference",
  },
  description:
    "Auto-generated reference for the Kitaru Python SDK and command-line interface.",
  icons: {
    icon: "/favicon.svg",
  },
  openGraph: {
    siteName: "Kitaru SDK & CLI Reference",
    type: "website",
  },
  twitter: {
    card: "summary_large_image",
  },
};

export default function Layout({ children }: LayoutProps<"/">) {
  return (
    <html lang="en" className={dmSans.variable} suppressHydrationWarning>
      <head>
        <script
          async
          src="https://plausible.io/js/pa-ndWEQcsUsU-PbMUClN4jg.js"
        />
        <script
          // biome-ignore lint/security/noDangerouslySetInnerHtml: static Plausible bootstrap, no user-controlled input
          dangerouslySetInnerHTML={{
            __html:
              "window.plausible=window.plausible||function(){(plausible.q=plausible.q||[]).push(arguments)};plausible.init=plausible.init||function(i){plausible.o=i||{}};plausible.init()",
          }}
        />
        <script
          // biome-ignore lint/security/noDangerouslySetInnerHtml: static reb2b loader, no user-controlled input
          dangerouslySetInnerHTML={{
            __html:
              '!function(key){if(window.reb2b)return;window.reb2b={loaded:true};var s=document.createElement("script");s.async=true;s.src="https://b2bjsstore.s3.us-west-2.amazonaws.com/b/"+key+"/"+key+".js.gz";document.getElementsByTagName("script")[0].parentNode.insertBefore(s,document.getElementsByTagName("script")[0])}("Z6PVLHP07Q6R")',
          }}
        />
      </head>
      <body className="flex flex-col min-h-screen">
        {/* biome-ignore lint/performance/noImgElement: 1px tracking pixel; image optimization does not apply */}
        <img
          referrerPolicy="no-referrer-when-downgrade"
          src="https://static.scarf.sh/a.png?x-pxid=ce7012ba-8b28-4529-8614-c7bafaf20f72"
          alt=""
          aria-hidden="true"
          width="1"
          height="1"
          style={{ position: "absolute" }}
        />
        <Provider>{children}</Provider>
      </body>
    </html>
  );
}
