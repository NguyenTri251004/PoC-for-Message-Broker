import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  scenarios: {
    async_100: {
      executor: 'constant-vus',
      vus: 100,
      duration: '20s',
    },
    async_500: {
      executor: 'constant-vus',
      vus: 500,
      duration: '20s',
    },
    async_1000: {
      executor: 'constant-vus',
      vus: 1000,
      duration: '20s',
    },
  },
};

export default function () {
  const payload = {
    customerId: `customer-${__VU}-${__ITER}`,
    productId: 'sku-001',
    quantity: 1,
    totalAmount: 249.99,
  };

  const res = http.post('http://localhost:3000/orders', JSON.stringify(payload), {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    'status is 2xx or 3xx': (r) => r.status >= 200 && r.status < 400,
  });

  sleep(0.05);
}
