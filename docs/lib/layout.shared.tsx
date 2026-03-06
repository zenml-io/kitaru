import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';

export const gitConfig = {
  user: 'zenml-io',
  repo: 'kitaru',
  branch: 'develop',
};

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: 'Kitaru',
    },
    githubUrl: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
  };
}
