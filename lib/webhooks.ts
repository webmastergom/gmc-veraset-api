/**
 * Webhook notification service for external API clients.
 * Sends status change notifications to registered webhook URLs.
 */

import { Job } from './jobs';
import { logger } from './logger';

interface WebhookPayload {
  event: 'job.status_changed';
  job_id: string;
  status: string;
  previous_status: string;
  timestamp: string;
  results_url: string;
}

/**
 * Send a webhook notification when job status changes.
 * Fire-and-forget with 1 retry on failure.
 */
export async function notifyWebhook(
  job: Job,
  oldStatus: string,
  newStatus: string
): Promise<void> {
  if (!job.webhookUrl) return;

  const payload: WebhookPayload = {
    event: 'job.status_changed',
    job_id: job.jobId,
    status: newStatus,
    previous_status: oldStatus,
    timestamp: new Date().toISOString(),
    results_url: `/api/external/jobs/${job.jobId}/status`,
  };

  const sendRequest = async (): Promise<boolean> => {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000);

      const response = await fetch(job.webhookUrl!, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': 'Veraset-API-Webhook/1.0',
        },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });

      clearTimeout(timeout);

      if (response.ok) {
        logger.log(`Webhook sent to ${job.webhookUrl}`, { jobId: job.jobId, status: newStatus });
        return true;
      }

      logger.warn(`Webhook failed: ${response.status}`, { jobId: job.jobId, url: job.webhookUrl });
      return false;
    } catch (err: any) {
      logger.warn(`Webhook error: ${err.message}`, { jobId: job.jobId, url: job.webhookUrl });
      return false;
    }
  };

  // First attempt
  const success = await sendRequest();

  // Retry once on failure
  if (!success) {
    await new Promise(resolve => setTimeout(resolve, 2000));
    await sendRequest();
  }
}
