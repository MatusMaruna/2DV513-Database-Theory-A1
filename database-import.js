var mysql = require('mysql')
var LineByLineReader = require('line-by-line')
var fs = require('fs')

class DatabaseImport {
  start () {
    this.stream = fs.createReadStream('RC_2011-07.json')
    this.lr = new LineByLineReader(this.stream)
    this.d = ''
    this.total = 0
    this.initilize()
    this.readDataToArray()
  }
  initilize () {
    this.database = ''
    this.subredditArray = []
    this.linkArray = []
    this.commentArray = []
    this.chunkedSubreddit = []
    this.chunkedLink = []
    this.chunkedComment = []
    this.connect()
  }
  async readDataToArray () {
    var index = 0
    this.d = new Date()
    console.log('Start Time : ' + this.d.getHours() + ':' + this.d.getMinutes() + ':' + this.d.getSeconds() + ':' + this.d.getMilliseconds())
    this.subredditArray = []
    this.linkArray = []
    this.commentArray = []
    this.lr.on('line', async (line) => {
      let data = JSON.parse(line)
      process.stdout.write('Reading line: ' + index++ + '\r')
      this.subredditArray.push([ data.subreddit_id, data.subreddit ])
      this.linkArray.push([ data.link_id.split('_')[1], data.link_id, data.subreddit_id ])
      this.commentArray.push([ data.id, data.name, data.author, data.score, data.body, data.subreddit_id, data.parent_id, new Date(parseInt(data.created_utc) * 1000), data.link_id.split('_')[1] ])
      if (index > 100000) {
        console.log('\n' + 'Array limit reached ! Querying !')
        this.lr.pause()
        await this.print()
        index = 0
        this.subredditArray.length = 0
        this.linkArray.length = 0
        this.commentArray.length = 0
        console.log('\n' + 'Done ! Continuing to read...')
        // this.initilize()
        setTimeout(() => {
          global.gc()
          this.lr.resume()
        }, 500)
      }
    })
    this.lr.on('end', () => this.print())
  }
  async print () {
    console.log('\n')
    this.total = this.total + this.commentArray.length
    this.chunkedSubreddit = this.chunk(this.subredditArray, 50000)
    this.chunkedLink = this.chunk(this.linkArray, 50000)
    this.chunkedComment = this.chunk(this.commentArray, 50000)
    console.log('CHECK: ' + this.chunkedComment.length + ':' + this.commentArray.length)
    for (let i = 0; i < this.chunkedSubreddit.length; i++) {
      process.stdout.write('Inserting subreddit chunk: ' + i + '\r')
      this.database.query('INSERT IGNORE INTO Subreddit (id, name) VALUES ?', [this.chunkedSubreddit[i]])
    }
    console.log('\n')
    for (let i = 0; i < this.chunkedLink.length; i++) {
      process.stdout.write('Inserting link chunk: ' + i + '\r')
      this.database.query('INSERT IGNORE INTO Link (id, name, subreddit_id) VALUES ?', [this.chunkedLink[i]])
    }
    console.log('\n')
    for (let i = 0; i < this.chunkedComment.length; i++) {
      process.stdout.write('Inserting comment chunk: ' + i + '\r')
      this.database.query('INSERT IGNORE INTO Comment (id, name, author, score, body, subreddit_id, parent_id, created_utc, link_id) VALUES ?', [this.chunkedComment[i]])
    }
    console.log('\n')
    this.chunkedSubreddit.length = 0
    this.chunkedLink.length = 0
    this.chunkedComment.length = 0
    let s = new Date()
    console.log('Start Time : ' + this.d.getHours() + ':' + this.d.getMinutes() + ':' + this.d.getSeconds() + ':' + this.d.getMilliseconds())
    console.log('End Time : ' + s.getHours() + ':' + s.getMinutes() + ':' + s.getSeconds() + ':' + s.getMilliseconds())
    console.log('Total elements : ' + this.total)
  }

  chunk (arr, size) {
    var index = 0
    var retArray = []
    retArray.length = 0

    for (index = 0; index < arr.length; index += size) {
      let chunk = arr.slice(index, index + size)
      retArray.push(chunk)
    }

    return retArray
  }

  connect () {
    this.database = mysql.createConnection({ host: 'mm223fj.mynetgear.com', user: 'jb223pt', password: 'Avyq3Gm', database: 'Reddit_nocon', port: '20124' })
    this.database.connect((err) => {
      if (err) {
        console.log('Could not connect to database!')
      } else {
        console.log('\nConnected!')
      }
    })
  }
}
module.exports = new DatabaseImport()
