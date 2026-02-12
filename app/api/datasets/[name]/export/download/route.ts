import { NextRequest, NextResponse } from 'next/server';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from '@/lib/s3-config';
import { isAuthenticated } from '@/lib/auth';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

export async function GET(
  req: NextRequest,
  context: { params: Promise<{ name: string }> | { name: string } }
) {
  if (!isAuthenticated(req)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const params = await (typeof context.params === 'object' && context.params instanceof Promise
    ? context.params
    : Promise.resolve(context.params as { name: string }));
  const datasetName = params.name;

  try {
    const searchParams = req.nextUrl.searchParams;
    const fileName = searchParams.get('file');
    
    if (!fileName) {
      return NextResponse.json(
        { error: 'File parameter required' },
        { status: 400 }
      );
    }
    
    // Validate filename to prevent path traversal
    if (fileName.includes('..') || fileName.includes('/')) {
      return NextResponse.json(
        { error: 'Invalid filename' },
        { status: 400 }
      );
    }

    // Validate that the file belongs to this dataset
    if (!fileName.startsWith(`${datasetName}-`)) {
      return NextResponse.json(
        { error: 'File does not belong to this dataset' },
        { status: 403 }
      );
    }
    
    const key = `exports/${fileName}`;
    
    // Download file from S3
    const command = new GetObjectCommand({
      Bucket: BUCKET,
      Key: key,
    });
    
    const response = await s3Client.send(command);
    
    if (!response.Body) {
      return NextResponse.json(
        { error: 'File not found' },
        { status: 404 }
      );
    }
    
    // Convert stream to text
    const chunks: Uint8Array[] = [];
    const stream = response.Body as any;
    
    for await (const chunk of stream) {
      chunks.push(chunk);
    }
    
    const content = Buffer.concat(chunks).toString('utf-8');
    
    // Return as CSV download
    return new NextResponse(content, {
      headers: {
        'Content-Type': 'text/csv',
        'Content-Disposition': `attachment; filename="${fileName}"`,
      },
    });
    
  } catch (error: any) {
    console.error('Download error:', error);
    
    if (error.name === 'NoSuchKey') {
      return NextResponse.json(
        { error: 'File not found' },
        { status: 404 }
      );
    }
    
    return NextResponse.json(
      { error: 'Download failed', details: error.message },
      { status: 500 }
    );
  }
}
