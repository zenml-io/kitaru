import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import Image from 'next/image';

export const gitConfig = {
  user: 'zenml-io',
  repo: 'kitaru',
  branch: 'develop',
};

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <>
          <Image src="/favicon.svg" alt="Kitaru" width={24} height={24} />
          <span style={{ fontWeight: 600 }}>Kitaru</span>
        </>
      ),
    },
    githubUrl: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
  };
}
