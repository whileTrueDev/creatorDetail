const pool = require('../model/connectionPool');
const { doQuery, doConnectionQuery, doTransacQuery } = require('../model/doQuery');
const updateFollower = require('./updateFollower.afreeca');

let startDate;
const ROWS_PER_ONE_HOUR = 20; // 아프리카 데이터 수집의 경우, 3분에 1회 수집하므로 3 * 20 = 60분
const forEachPromise = (items, fn) => items.reduce((promise, item) => promise.then(() => fn(item)), Promise.resolve());

/**
 * 해당 크리에이터의 시청자 수를 집계하는 함수
 * @param {object} param0 connection, creatorId, afreecaId
 * @author chan
 * @maintainer hwasurr 21. 01. 14
 */
const getViewerAirtime = ({ connection, creatorId, afreecaId }) => {
  const selectQuery = `
  SELECT B.broadId, AVG(viewCount) AS viewer, COUNT(*) AS airtime, MAX(viewCount) AS peakview
  FROM
    (SELECT broadId FROM AfreecaBroad WHERE userId = ? AND broadStartedAt > ?)
  AS B
  LEFT JOIN AfreecaBroadDetail AS A ON A.broadId = B.broadId
  GROUP BY broadId
  `;

  const removeOutliner = ({ streamData }) => {
    const cutStreamData = streamData.reduce((acc, element) => {
      // 한시간 이하의 방송은 이상치이므로 제거한다.
      if (element.airtime > ROWS_PER_ONE_HOUR) {
        acc.push(element);
      }
      return acc;
    }, []);

    if (cutStreamData.length === 0) {
      return {};
    }
    const viewerList = [];
    const airtimeList = [];
    const peakList = [];
    const average = (data) => data.reduce((sum, value) => sum + value) / data.length;
    
    cutStreamData.forEach(({ viewer, airtime, peakview }) => {
      peakList.push(peakview);
      airtimeList.push(airtime);
      viewerList.push(viewer);
    });

    // 시간의 경우, ROW의 갯수를 COUNT하여 값을 가져오는데, 10분당 3개를 찍으므로 3으로 나눈다.
    // 3개당 10분 이므로, 3을 나눈 갯수 * 10이 실제시간이 되고, 60으로 나누면 시간이 나오게 된다. 평균 방송시간이 나오게 된다.
    const airtime = Number((average(airtimeList) / (ROWS_PER_ONE_HOUR)).toFixed(1));
    const viewer = (Math.round(average(viewerList)));
    const peakview = Math.max(...peakList);

    return { airtime, viewer, peakview };
  };

  return new Promise((resolve, reject) => {
    doConnectionQuery({ connection, queryState: selectQuery, params: [afreecaId, startDate] })
      .then((row) => {
        if (row.length > 0) {
          const viewData = removeOutliner({ streamData: row });
          resolve(viewData);
        } else {
          resolve({});
        }
      })
      .catch((err) => {
        console.log(err);
        console.log(`${creatorId}의 시청자수, 방송시간을 구하는 과정`);
        resolve({});
      });
  });
};

/**
 * 해당 크리에이터의 주 방송 카테고리를 분석하는 함수
 * @param {object} param0 connection, creatorId, afreecaId
 * @author chan
 * @maintainer hwasurr 21. 01. 14
 */
const getContentsPercent = ({ connection, creatorId, afreecaId }) => {
  const selectQuery = `
  SELECT categoryNameKr AS gameName, categoryId AS gameId, timecount
  FROM (
    SELECT broadCategory, count(*) AS timecount
    FROM AfreecaBroadDetail AS A
    LEFT JOIN (
      SELECT broadId, userId
      FROM AfreecaBroad
      WHERE broadStartedAt > ?
    ) AS B
    ON A.broadId = B.broadId
    WHERE B.userId = ?
    GROUP BY broadCategory ) AS C
  LEFT JOIN AfreecaCategory ON C.broadCategory = AfreecaCategory.categoryId
  `;

  const preprocessing = (percentData) => {
    const outputData = percentData;
    const sum = percentData.reduce((a, b) => a + b.timecount, 0);

    percentData.forEach((element, index) => {
      const percent = Number((element.timecount / sum).toFixed(2));
      outputData[index].percent = percent;
    });

    outputData.sort((a, b) => b.timecount - a.timecount);

    let cumsum = 0;
    const returnData = outputData.reduce((result, element) => {
      if (cumsum <= 0.8) {
        cumsum += element.percent;
        result.push({
          gameName: element.gameName,
          percent: element.percent
        });
      }
      return result;
    }, []);

    return returnData;
  };

  return new Promise((resolve, reject) => {
    doConnectionQuery({ connection, queryState: selectQuery, params: [startDate, afreecaId] })
      .then((row) => {
        if (row.length > 0) {
          const contentsGraphData = preprocessing(row);
          const content = contentsGraphData[0].gameName;
          resolve({ content, contentsGraphData });
        } else {
          console.log(`${creatorId}의 방송시간대를 구하는 과정 (getContentsPercent) - empty`);
          resolve({ content: '', contentsGraphData: {} });
        }
      })
      .catch((err) => {
        console.log('--------------------');
        console.error(err);
        console.error(`ERROR - ${creatorId}의 방송시간대를 구하는 과정 (getContentsPercent)`);
        console.log('--------------------');
        resolve({ content: '', contentsGraphData: {} });
      });
  });
};

/**
 * 해당 크리에이터의 평균 방송 시간과 방송 시간 그래프 데이터를 생성하는 함수
 * @param {object} param0 connection, creatorId, afreecaId
 * @author chan
 * @maintainer hwasurr 21. 01. 14
 */
const getOpenHourPercent = ({ connection, creatorId, afreecaId }) => {
  const getTimeName = (time) => {
    if (time >= 0 && time < 6) {
      return '새벽';
    }
    if (time >= 6 && time < 12) {
      return '오전';
    }
    if (time >= 12 && time < 18) {
      return '오후';
    }
    if (time >= 18 && time < 24) {
      return '저녁';
    }
  };

  const countTime = ({ timeData }) => {
    const timeDict = {
      새벽: 0,
      오전: 0,
      오후: 0,
      저녁: 0,
    };
    timeData.forEach((element) => {
      const timeName = getTimeName(element.hours);
      timeDict[timeName] += element.sumtime;
    });
    const timeList = Object.keys(timeDict).map((dateName) => {
      const count = timeDict[dateName];
      return {
        name: dateName,
        count
      };
    });
    timeList.sort((a, b) => b.count - a.count);
    return `${timeList[0].name}, ${timeList[1].name}`;
  };

  const normalization = ({ timeData }) => {
    let outData = [];

    if (timeData.length !== 24) {
      outData = [...Array(24).keys()].map((i) => ({
        hours: i,
        sumtime: 0
      }));
      timeData.forEach((element) => {
        const hour = element.hours;
        outData[hour].sumtime = element.sumtime;
      });
    } else {
      outData = timeData;
    }


    // 일단 크기순으로 sorting (24개의 원소이므로 굉장히 작은 연산이다.)
    outData.sort((a, b) => a.sumtime - b.sumtime);

    const min = outData[0].sumtime;
    const max = outData[23].sumtime;
    outData = outData.map((element) => {
      const sumtime = Number(((element.sumtime - min) / (max - min)).toFixed(2));
      return {
        ...element,
        sumtime
      };
    });

    outData.sort((a, b) => a.hours - b.hours);
    const returArray = outData.concat(outData.splice(0, 6));

    return returArray;
  };

  const timeQuery = `
  SELECT COUNT(*) AS sumtime, HOUR(A.createdAt) AS hours
    FROM AfreecaBroadDetail AS A
  LEFT JOIN (
    SELECT broadId, userId
    FROM AfreecaBroad WHERE broadStartedAt > ?
  ) AS B
  ON A.broadId = B.broadId
    WHERE B.userId = ?
    GROUP BY HOUR(A.createdAt)
  `;

  return new Promise((resolve, reject) => {
    doConnectionQuery({ connection, queryState: timeQuery, params: [startDate, afreecaId] })
      .then((row) => {
        if (row.length > 0) {
          const openHour = countTime({ timeData: row });
          const timeGraphData = normalization({ timeData: row });
          resolve({ openHour, timeGraphData });
        } else {
          console.log(`${creatorId}의 방송시간대를 구하는 과정 (getOpenHourPercent) - empty`);
          resolve({ openHour: '', timeGraphData: {} });
        }
      })  
      .catch((err) => {
        console.log('--------------------');
        console.error(err);
        console.error(`ERROR - ${creatorId}의 방송시간대를 구하는 과정 (getOpenHourPercent)`);
        console.log('--------------------');
        resolve({ openHour: '', timeGraphData: {} });
      });
  });
};

/**
 * 해당 크리에이터의 일간 평균 클릭수를 집계 및 계산하는 함수
 * @param {object} param0 connection, creatorId
 * @author chan
 */
const getClickPercent = ({ connection, creatorId }) => {
  const clickQuery = `
  SELECT ROUND(avg(counts), 2) AS counts
  FROM (SELECT COUNT(*) AS counts, DATE(date)
  FROM landingClickIp
  WHERE creatorId = ?
  AND landingClickIp.type = 2
  GROUP BY DATE(date)) AS C
  `;

  return new Promise((resolve, reject) => {
    doConnectionQuery({ connection, queryState: clickQuery, params: [creatorId] })
      .then((row) => {
        if (row.length > 0) {
          const clickCount = row[0].counts;
          resolve({ clickCount });
        } else {
          console.log(`${creatorId}의 클릭률을 구하는 과정`);
          resolve({ clickCount: 0 });
        }
      })
      .catch((err) => {
        console.log('--------------------');
        console.error(err);
        console.error(`ERROR - ${creatorId}의 클릭률을 구하는 과정`);
        console.log('--------------------');
        resolve({ clickCount: 0 });
      });
  });
};

/**
 * 해당 크리에이터가 실제 방송을 진행한 총 방송 시간을 구하는 함수
 * getTimeData 함수 내부에서 RIP를 구하기 위해 사용됩니다.
 * @param {object} param0 connection, creatorId, afreecaId
 * @author chan
 * @maintainer hwasurr 21. 01. 14
 */
const getAirtime = ({ connection, creatorId, afreecaId }) => new Promise((resolve, reject) => {
  const selectQuery = `
  SELECT  COUNT(*) AS airtime
    FROM AfreecaBroadDetail AS A
  LEFT JOIN AfreecaBroad AS B 
    ON A.broadId = B.broadId
  WHERE B.userId = ? AND B.createdAt > ?
  `;
  // 실제 노출시간의 경우, 반드시 3으로 나누어줘야함.
  // Number((average(airtimeList) / (6 * 3)).toFixed(1));

  doConnectionQuery({ connection, queryState: selectQuery, params: [afreecaId, startDate] })
    .then((row) => {
      const { airtime } = row[0];
      resolve(Number(Math.round(Number(airtime) / 3)));
    })
    .catch((err) => {
      console.log('--------------------');
      console.error(err);
      console.error(`ERROR - ${creatorId}의 실제 방송 시간을 구하는 과정`);
      console.log('--------------------');
      resolve(0);
    });
});

/**
 * 해당 크리에이터의 실제 광고 노출 시간을 구하는 함수.
 * getTimeData 함수 내부에서 RIP를 구하기 위해 사용됩니다.
 * @param {object} param0 connection, creatorId
 * @author chan
 * @maintainer hwasurr 21. 01. 14
 */
const getImpressiontime = ({ connection, creatorId }) => new Promise((resolve, reject) => {
  const selectQuery = `
  SELECT  COUNT(*) AS impressiontime
    FROM campaignLog
    WHERE creatorId = ? AND type = 'CPM' AND date > ?
  `;

  doConnectionQuery({ connection, queryState: selectQuery, params: [creatorId, startDate] })
    .then((row) => {
      const { impressiontime } = row[0];
      resolve(impressiontime);
    })
    .catch((err) => {
      console.log('--------------------');
      console.error(err);
      console.error(`ERROR - ${creatorId}의 노출시간을 구하는 과정`);
      console.log('--------------------');
      resolve(0);
    });
});

/**
 * 해당 크리에이터의 RIP 를 구하는 함수
 * "RIP" 란, 총 방송시간 중, 온애드 배너를 띄우고 있던 시간을 0 에서 1 사이의 숫자로 나타낸 값이다.
 * @param {object} param0 connection, creatorId, afreecaId
 * @author chan
 * @maintainer hwasurr 21. 01. 14
 */
const getTimeData = ({ connection, creatorId, afreecaId }) => new Promise((resolve, reject) => {
    Promise.all([
      getAirtime({ connection, creatorId, afreecaId }),
      getImpressiontime({ connection, creatorId })
    ])
      .then(([airtime, impressiontime]) => {
        let RIP = 0;
        if (airtime !== 0) {
          RIP = Number((impressiontime / airtime).toFixed(2));
          if (RIP >= 1) RIP = 1;
        } else {
          RIP = 0;
        }
        resolve(RIP);
      })
      .catch((error) => {
        console.log('streamDetail에서 데이터를 가져오는 과정');
        console.log(error);
        resolve(0);
      });
});

/**
 * 해당 크리에이터의 이미 적재되어 있는 createDetailAfreeca 행을 가져오는 함수.
 * @param {object} param0 connection, creatorId
 * @author chan
 */
const getCheckDetail = ({ connection, creatorId }) => new Promise((resolve, reject) => {
  const selectQuery = `
  SELECT * FROM creatorDetailAfreeca WHERE creatorId = ?
  `;

  doConnectionQuery({ connection, queryState: selectQuery, params: [creatorId] })
    .then((row) => {
      if (row.length > 0) {
        resolve(true);
      } else {
        resolve(false);
      }
    })
    .catch(() => {
      resolve(true);
    });
});

/**
 * 각 타겟 크리에이터 개별에 대해 지표 분석을 시작하는 함수입니다. dividedGetDetail 내부의 크리에이터 목록 반복문 안에서 사용됩니다.
 * @param {object} param0 connection, creatorId, afreecaId
 * @author chan
 * @maintainer hwasurr 21. 01. 14
 */
const getCreatorDetail = ({ connection, creatorId, afreecaId }) => new Promise((resolve, reject) => {
  getViewerAirtime({ connection, creatorId, afreecaId })
    .then((viewData) => {
      resolve();
      if (Object.entries(viewData).length === 0 && viewData.constructor === Object) {
        resolve();
        return;
      }
      const { airtime, viewer, peakview } = viewData;

      Promise.all([
        getClickPercent({ connection, creatorId }),
        getOpenHourPercent({ connection, creatorId, afreecaId }),
        getContentsPercent({ connection, creatorId, afreecaId }),
        getTimeData({ connection, creatorId, afreecaId }),
      ])
        .then(([{ clickCount }, { openHour, timeGraphData }, { content, contentsGraphData }, rip]) => {
          if (viewer === 0 || airtime === 0 || rip == 0) {
            return [];
          }
          const impression =  Math.ceil(airtime * viewer * 6 * rip);
          const followers = 0;
          const ctr = Math.ceil(clickCount);
          const cost =  Math.ceil(viewer * 6 * 2 * rip);
          const timeJsonData = JSON.stringify({ data: timeGraphData });
          const contentsJsonData = JSON.stringify({ data: contentsGraphData });
          return [
            followers,
            ctr,
            airtime,
            viewer,
            impression,
            cost,
            content,
            openHour,
            timeJsonData,
            contentsJsonData,
            peakview,
            rip,
            creatorId
          ];
        })
        .then((params) => {
          if (params.length === 0) {
            getCheckDetail({ connection, creatorId })
            .then((check) => {
              const queryState = `
              DELETE FROM creatorDetailAfreeca WHERE creatorId = ?
              `;
              if(check)
              {
                doTransacQuery({ connection, queryState, params: [creatorId] })
                .then(() => {
                  resolve();
                })
                .catch((error) => {
                  console.error(error);
                  console.error(`ERROR - ${creatorId}(${afreecaId})의 데이터를 저장하는 과정`);
                  resolve();
                });
              }else {
                resolve();
              }
            });
          }else {
            getCheckDetail({ connection, creatorId })
            .then((check) => {
              const insertQuery = `
              INSERT INTO creatorDetailAfreeca
              (followers, ctr, airtime, viewer, impression, cost, content, openHour, timeGraphData, contentsGraphData, peakview, rip, creatorId)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              `;
              const updateQuery = `
              UPDATE creatorDetailAfreeca
              SET 
              ctr = ?, 
              airtime = ?, 
              viewer = ?, 
              impression = ?,
              cost = ?,
              content = ?, 
              openHour = ?,
              timeGraphData = ?,
              contentsGraphData = ?, 
              peakview = ?, 
              rip = ?
              WHERE creatorId = ?
              `;
              const queryState = check ? updateQuery : insertQuery;
              const newParams = check ? params.slice(1) : params;

              doTransacQuery({ connection, queryState, params: newParams })
                .then(() => {
                  console.log(`[Afreeca] ${creatorId}(${afreecaId}) creatorDetailAfreeca INSERT OR UPDATE 완료`);
                  resolve();
                })
                .catch((error) => {
                  console.error(error);
                  console.error(`ERROR - ${creatorId}(${afreecaId})의 데이터를 저장하는 과정`);
                  resolve();
                });
            });
          }
        });
    })
    .catch((error) => {
      console.error(error);
      resolve();
    });
});

/**
 * 나누어진 크리에이터 목록에 대한 지표 분석 작업을 시작하는 함수입니다.
 * @param {object} targetList 지표 분석 작업을 시작할 크리에이터 목록
 * @author chan
 * @maintainer hwasurr 21. 01. 14
 */
const dividedGetDetail = (targetList) => new Promise((resolve, reject) => {
  pool.getConnection((err, connection) => {
    if (err) {
      console.log(err);
      reject(err);
    } else {
      Promise.all(
        targetList.map((creator) => {
          console.log(`${creator.creatorId}(${creator.afreecaId}) 지표 분석 실행`)
          getCreatorDetail({
            connection,
            creatorId: creator.creatorId,
            afreecaId: creator.afreecaId
          })
        })
      ).then(() => {
        connection.release();
        setTimeout(() => {
          resolve();
        }, 10000);
      });
    }
  });
});

/**
 * 지표 분석 타겟 BJ 목록을 불러옵니다.
 * 온애드 가입 + 온애드 이용약관 동의 + 온애드 연동을 완료한 크리에이터의 afreecaId, creatorId를 모두 불러옵니다.
 * @author chan
 * @maintainer hwasurr 21. 01. 14
 */
const getDoQueryCreatorList = () => new Promise((resolve, reject) => {
  const date = new Date();
  date.setDate(date.getDate() - 7);
  const selectQuery = `
  SELECT creatorId, afreecaId 
    FROM creatorInfo
  WHERE date < ?
    AND creatorContractionAgreement = 1
    AND afreecaId IS NOT NULL
  `;
  doQuery(selectQuery, [date])
    .then((row) => {
      const creatorList = row.result;
      resolve(creatorList);
    })
    .catch((err) => {
      console.log('--------------------');
      console.error(err);
      console.error('ERROR - creator list를 가져오는 과정');
      console.log('--------------------');
      resolve([]);
    });
});

/**
 * 분석 실행 함수. 크리에이터 목록을 30명씩 분할하여 실행한다. (성능문제로)
 * @author chan
 */
const analyzeStart = async () => {
  const creatorList = await getDoQueryCreatorList();
  const turns = Math.ceil(creatorList.length / 30);
  const targetList = [];
  for (let i = 0; i < turns;) {
    const targets = creatorList.splice(0, 30);
    targetList.push(targets);
    i += 1;
  }
  return new Promise((resolve, reject) => {
    forEachPromise(targetList, dividedGetDetail)
      .then(() => {
        resolve();
      });
  });
};

/**
 * 지표 분석에 사용되는 날짜 변수를 생성하는 함수.
 * @author chan
 */
const setStartDate = () => {
  function pad(n, width) {
    n = n + '';
    return n.length >= width ? n : new Array(width - n.length + 1).join('0') + n;
  }
  const date = new Date();
  date.setMonth(date.getMonth()- 1);
  date.setDate(1);
  return `${date.getFullYear()}-${pad(date.getMonth(), 2)}-${pad(date.getDate(), 2)} 00:00:00`;
}


async function main() {
  console.log('=========================================');
  console.log('Afreeca creator detail - data UPDATE START');
  console.log('=========================================');
  startDate = setStartDate();
 
  await analyzeStart();
  await updateFollower();
  console.log('=========================================');
  console.log('Afreeca creator detail - data update complete!');
  console.log('=========================================');

}

module.exports = main;