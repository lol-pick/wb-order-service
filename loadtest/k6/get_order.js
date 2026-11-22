// Нагрузочный тест GET /order/{order_uid}.
// order_uid берутся из ids.txt (выгружается командой: go run ./cmd/loadgen ids -n 1000).
//
//   k6 run loadtest/k6/get_order.js
//   k6 run -e RATE=2000 -e DURATION=1m loadtest/k6/get_order.js
//
// Переменные: BASE_URL (по умолчанию http://localhost:8081), RATE (запросов/с, 500), DURATION (30s).
import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';

// SharedArray читает файл один раз и делит его между всеми виртуальными пользователями
const ids = new SharedArray('ids', () =>
  open('./ids.txt').split('\n').filter((s) => s.trim() !== ''),
);

export const options = {
  // Постоянная частота запросов (open model): k6 держит RATE запросов/с независимо от того,
  // как быстро отвечает сервис. Так задержки сравнимы между прогонами.
  scenarios: {
    get_order: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 500),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: 50,
      maxVUs: 500,
    },
  },
  // Какие перцентили показать в итоговой таблице
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    http_req_failed: ['rate<0.01'], // меньше 1% ошибок, иначе прогон считается проваленным
  },
};

// setup выполняется один раз до нагрузки: запрашиваем каждый заказ,
// чтобы при включённом кэше замер шёл по «тёплому» кэшу
export function setup() {
  for (const id of ids) {
    http.get(`${BASE_URL}/order/${id}`, { tags: { name: 'warmup' } });
  }
}

export default function () {
  const id = ids[Math.floor(Math.random() * ids.length)];
  // tags.name группирует все запросы под одним именем вместо тысячи разных URL
  const res = http.get(`${BASE_URL}/order/${id}`, { tags: { name: 'GET /order/{uid}' } });
  check(res, { 'status is 200': (r) => r.status === 200 });
}
