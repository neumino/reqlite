import {Server, connect, rethinkdb as r} from '../dist/browser';

process.stderr = {
  write: ::console.log
};


var server = new Server({
  'driver-port': 28015
});



function promise(...args){
  return new Promise((resolve, reject) => {
    return this(...args, (err, res) => err ? reject(err) : resolve(res));
  });
}

function run(conn){
  return (::this.run)::promise(conn);
}

function toArray(){
  return (::this.toArray)::promise();
}


async function go(){
  try{
    let conn = await connect({port: 28015}); // "fake" port
    await r.dbCreate('test')::run(conn);
    await r.db('test').tableCreate('samples')::run(conn);
    for (var i=0; i< 100; i++){
      await r.db('test').table('samples').insert({
        value: `this value was ${Math.round(Math.random()*1000)}`
      })::run(conn);
    }

    let cursor = await r.db('test').table('samples')::run(conn);

    if(cursor){
      let arr = await cursor::toArray();
      console.log(arr.map(x => `${x.id}: ${x.value}`).join('\n'));
    }
    else{
      console.log('no results found');
    }
  }
  catch(e){
    console.error('fail!', e.stack);
  }
}

go();












