import { getApiHealthStatus } from '../health.mjs';

export default async function handler(req, res) {
  try {
    const health = await getApiHealthStatus({
      source: 'vercel-function'
    });

    res.status(200).json(health);
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: 'Health check failed',
      message: error.message
    });
  }
}
