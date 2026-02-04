import { S3Client, ListObjectsV2Command, CopyObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';

const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
});

export async function listS3Objects(bucket: string, prefix: string) {
  const command = new ListObjectsV2Command({
    Bucket: bucket,
    Prefix: prefix,
  });
  
  const response = await s3Client.send(command);
  return response.Contents || [];
}

export async function copyS3Object(
  sourceBucket: string,
  sourceKey: string,
  destBucket: string,
  destKey: string
) {
  const command = new CopyObjectCommand({
    CopySource: `${sourceBucket}/${sourceKey}`,
    Bucket: destBucket,
    Key: destKey,
  });
  
  return s3Client.send(command);
}

export async function getObjectMetadata(bucket: string, key: string) {
  const command = new HeadObjectCommand({
    Bucket: bucket,
    Key: key,
  });
  
  return s3Client.send(command);
}

export function parseS3Path(path: string): { bucket: string; key: string } {
  const match = path.match(/^s3:\/\/([^\/]+)\/(.+)$/);
  if (!match) {
    throw new Error(`Invalid S3 path: ${path}`);
  }
  return { bucket: match[1], key: match[2] };
}

/**
 * List all datasets (directories) in the bucket root
 */
export async function listDatasets(): Promise<string[]> {
  const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';
  const command = new ListObjectsV2Command({
    Bucket: BUCKET,
    Delimiter: '/',
  });
  
  const response = await s3Client.send(command);
  return (response.CommonPrefixes || []).map(prefix => prefix.Prefix?.replace('/', '') || '').filter(Boolean);
}
