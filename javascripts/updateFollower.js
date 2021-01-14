const axios = require('axios');
const { doQuery, doTransacQuery } = require('../model/doQuery');
const pool = require('../model/connectionPool');
const forEachPromise = (items, fn) => items.reduce((promise, item) => promise.then(() => fn(item)), Promise.resolve());

const getDetaildoQueryList = () => new Promise((resolve, reject) => {
  const selectQuery = `
  SELECT CI.creatorId, creatorTwitchOriginalId
  FROM creatorDetail AS CD
  JOIN creatorInfo AS CI
    ON CI.creatorId = CD.creatorId
  `;
  doQuery(selectQuery, [])
    .then((row) => {
      // 타겟 유저 정보
      // [{ creatorTwitchOriginalId: 'asdf', creatorId: 'asdf' }, { ... } ,... ]
      const creatorList = row.result;
      resolve(creatorList);
    })
    .catch((err) => {
      console.log('creator list를 가져오는 과정');
      console.log(err);
      resolve([]);
    });
});

const UpdateFollower = ({
  creatorId, connection, accessToken, creatorTwitchOriginalId
}) => new Promise((resolve, reject) => {
  const clientID = process.env.PRODUCTION_CLIENT_ID;

  const config = {
    headers: {
      'Client-ID' : `${clientID}`,
      'Authorization': `Bearer ${accessToken}`
    }
  };
  const url = `https://api.twitch.tv/helix/users/follows?to_id=${creatorTwitchOriginalId}`;
  axios.get(url, config)
    .then((res) => {
      const followers = res.data.total;
      const queryState = `
      UPDATE creatorDetail 
      SET followers = ?
      WHERE creatorId = ? 
      `;
      doTransacQuery({ connection, queryState, params: [followers, creatorId] })
        .then(() => {
          resolve();
        })
        .catch((error) => {
          console.log(`${creatorId}의 데이터를 저장하는 과정`);
          resolve();
        });
    })
    .catch((error) => {
      console.log('twitch API를 통한 구독자수 요청 실패');
      resolve();
    });
});


// 초기 팔로워수 업데이트 이전에 token 발급받기.
const getAccessToken = () => new Promise((resolve, reject) => {
  const clientID = process.env.PRODUCTION_CLIENT_ID;
  const clientSecret = process.env.PRODUCTION_CLIENT_SECRET;

  const authorizationUrl = ` https://id.twitch.tv/oauth2/token?client_id=${clientID}&client_secret=${clientSecret}&grant_type=client_credentials&scope=user:read:email`
  axios.post(authorizationUrl)
  .then((res) => {
    const accesstoken = res.data.access_token || '';
    resolve(accesstoken);
  })
  .catch((error) => {
    console.log('twitch API를 통한 token 가져오기 실패');
    resolve('');
  });
})

const dividedGetFollower = (targetList) => new Promise((resolve, reject) => {
  pool.getConnection(async (err, connection) => {
    if (err) {
      console.log(err);
    } else {
      // access token get 부
      const accessToken = await getAccessToken();
      if (accessToken == '') {
        resolve();
        return;
      }
      Promise.all(
        targetList.map((creator) => UpdateFollower({
          creatorId: creator.creatorId,
          creatorTwitchOriginalId: creator.creatorTwitchOriginalId,
          connection,
          accessToken
        }))
      ).then(() => {
        connection.release();
        setTimeout(() => {
          resolve();
        }, 60000);
      });
    }
  });
});

const dividedUpdate = async () => {
  console.log('=========================================');
  console.log('TWITCH follower UPDATE START.');
  console.log('=========================================');
  const creatorList = await getDetaildoQueryList();
  const turns = Math.ceil(creatorList.length / 30);
  const targetList = [];
  for (let i = 0; i < turns;) {
    const targets = creatorList.splice(0, 30);
    targetList.push(targets);
    i += 1;
  }


  //  토큰을 가져오지 못할경우 바로 리턴하여 종료한다.

  return new Promise((resolve, reject) => {
    forEachPromise(targetList, dividedGetFollower)
      .then(() => {
        resolve();
      });
  });
};

module.exports = dividedUpdate;