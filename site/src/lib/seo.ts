import { absoluteUrl, site } from '../config/site';

export interface Crumb {
  name: string;
  path: string;
}

export function breadcrumbLd(crumbs: Crumb[]) {
  return {
    '@context': 'https://schema.org',
    '@type': 'BreadcrumbList',
    itemListElement: [{ name: 'Home', path: '/' }, ...crumbs].map((c, i) => ({
      '@type': 'ListItem',
      position: i + 1,
      name: c.name,
      item: absoluteUrl(c.path),
    })),
  };
}

export function softwareLd(version: string) {
  return {
    '@context': 'https://schema.org',
    '@type': 'SoftwareApplication',
    name: site.name,
    description: site.description,
    applicationCategory: 'DeveloperApplication',
    operatingSystem: 'Linux',
    softwareVersion: version,
    license: site.licenseUrl,
    url: absoluteUrl('/'),
    downloadUrl: site.releasesUrl,
    codeRepository: site.repoUrl,
    offers: { '@type': 'Offer', price: 0, priceCurrency: 'USD' },
  };
}

export function blogPostingLd(p: {
  title: string;
  description: string;
  path: string;
  pubDate: Date;
  updatedDate?: Date;
  author: string;
}) {
  return {
    '@context': 'https://schema.org',
    '@type': 'BlogPosting',
    headline: p.title,
    description: p.description,
    datePublished: p.pubDate.toISOString(),
    dateModified: (p.updatedDate ?? p.pubDate).toISOString(),
    author: { '@type': 'Person', name: p.author },
    mainEntityOfPage: absoluteUrl(p.path),
    image: absoluteUrl(site.ogImage),
  };
}
