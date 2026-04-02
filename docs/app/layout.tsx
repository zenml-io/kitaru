import { DM_Sans, JetBrains_Mono } from 'next/font/google';
import { Provider } from '@/components/provider';
import type { Metadata } from 'next';
import './global.css';

const dmSans = DM_Sans({
  subsets: ['latin'],
  variable: '--font-dm-sans',
  display: 'swap',
});

const jetbrainsMono = JetBrains_Mono({
  subsets: ['latin'],
  variable: '--font-jetbrains-mono',
  display: 'swap',
  weight: ['400', '500'],
});

export const metadata: Metadata = {
  metadataBase: new URL('https://kitaru.ai/docs'),
  title: {
    default: 'Kitaru Documentation',
    template: '%s | Kitaru',
  },
  description:
    'Durable execution for AI agents. Primitives that make agent workflows persistent, replayable, and observable.',
  icons: {
    icon: '/favicon.svg',
  },
  openGraph: {
    siteName: 'Kitaru Documentation',
    type: 'website',
  },
  twitter: {
    card: 'summary_large_image',
  },
};

export default function Layout({ children }: LayoutProps<'/'>) {
  return (
    <html lang="en" className={`${dmSans.variable} ${jetbrainsMono.variable}`} suppressHydrationWarning>
      <head>
        <script
          async
          src="https://plausible.io/js/pa-ndWEQcsUsU-PbMUClN4jg.js"
        />
        <script
          dangerouslySetInnerHTML={{
            __html:
              'window.plausible=window.plausible||function(){(plausible.q=plausible.q||[]).push(arguments)};plausible.init=plausible.init||function(i){plausible.o=i||{}};plausible.init()',
          }}
        />
        <script
          dangerouslySetInnerHTML={{
            __html:
              '!function(key){if(window.reb2b)return;window.reb2b={loaded:true};var s=document.createElement("script");s.async=true;s.src="https://b2bjsstore.s3.us-west-2.amazonaws.com/b/"+key+"/"+key+".js.gz";document.getElementsByTagName("script")[0].parentNode.insertBefore(s,document.getElementsByTagName("script")[0])}("Z6PVLHP07Q6R")',
          }}
        />
      </head>
      <body className="flex flex-col min-h-screen">
        <img referrerPolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=ce7012ba-8b28-4529-8614-c7bafaf20f72" alt="" aria-hidden="true" width="1" height="1" style={{position:'absolute'}} />
        <Provider>{children}</Provider>
      </body>
    </html>
  );
}
