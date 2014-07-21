var helper = require(__dirname+"/helper.js");
var Error = require(__dirname+"/error.js");

function Sequence(sequence) {
    this.sequence = sequence || [];
    this.length = this.sequence.length;;
}

Sequence.prototype.push = function(element) {
    this.sequence.push(element);
    this.length++;
    return this;
}
Sequence.prototype.unshift = function(element) {
    this.sequence.unshift(element);
    this.length++;
    return this;
}
Sequence.prototype.concat = function(other) {
    var result = new Sequence();
    for(var i=0; i<this.sequence.length; i++) {
        result.push(this.sequence[i]);
    }
    other = other.toSequence();
    for(var i=0; i<other.sequence.length; i++) {
        result.push(other.sequence[i]);
    }
    return result;
}
Sequence.prototype.toSequence = function() {
    // Returns a new sequence
    var result = new Sequence();
    for(var i=0; i<this.sequence.length; i++) {
        result.push(this.sequence[i]);
    }
    return result;
}
Sequence.prototype.sample = function(sample) {
    if (sample > this.sequence.length) {
        return sequence.toSequence();
    }
    else {
        var result = new Sequence();
        var index;
        while (sample > 0) {
            index = Math.floor(Math.random()*this.sequence.length);
            result.push(this.sequence.splice(index, 1)[0]);
            sample--;
        }
        return result;
    }
}

Sequence.prototype.merge = function(toMerge, query) {
    var result = new Sequence();

    for(var i=0; i<this.sequence.length; i++) {
        result.push(helper.merge(this.sequence[i], toMerge, query))
    }

    return result;
}


Sequence.prototype.eqJoin = function(leftField, other, options, query) {
    // other is a table since eqJoin requires an index

    var result = new Sequence();

    // If leftFiend is a string, replace it with a function
    if (typeof leftField === "string") {
        var uuid = helper.uuid();
        leftField = [ 69, [ [ 2, [ uuid ] ], [ 31, [ [ 10, [ uuid ] ], leftField ] ] ] ]
    }

    var varId, leftFieldValue, partial;
    for(var i=0; i<this.sequence.length; i++) {
        varId = leftField[1][0][1][0]; //TODO Refactor

        query.context[varId] = helper.toDatum(this.sequence[i]);
        console.log("LEFTVALUE")
        leftFieldValue = query.evaluate(leftField);
        delete query.context[varId];

        //TODO Wrap in a try catch? Frames should not propagate here
        partial = other.getAll([leftFieldValue], options, query);
        console.log("=====")
        console.log("leftfieldvalue", leftFieldValue)
        console.log("PARTIAL", partial)

        for(var k=0; k<partial.selection.length; k++) {
            result.push({
                left: this.sequence[i],
                right: partial.selection[k]
            })
        }
    }
    return result;
}

Sequence.prototype.join = function(type, other, predicate, query) {
    var result = new Sequence();
    var varIds, predicateResult, returned;


    if (typeof other.toSequence === "function") {
        other = other.toSequence();
    }

    for(var i=0; i<this.sequence.length; i++) {
        returned = false; 
        for(var j=0; j<other.sequence.length; j++) {
            varIds = predicate[1][0][1]; //TODO Refactor
            query.context[varIds[0]] = helper.toDatum(this.sequence[i]);
            query.context[varIds[1]] = helper.toDatum(other.sequence[j]);

            predicateResult = query.evaluate(predicate);
            if (helper.isTrue(predicateResult)) {
                returned = true;
                result.push({
                    left: this.sequence[i],
                    right: other.sequence[j]
                });
            }
            delete query.context[varIds[0]];
            delete query.context[varIds[1]];
        }
        if ((type === 'outer') && (returned === false)) {
            result.push({
                left: this.sequence[i]
            });
        }
    }
    return result;
}

Sequence.prototype.filter = function(filter, options, query) {
    //TODO
    return new Sequence();
}

Sequence.prototype.pop = function(element) {
    this.length--;
    this.sequence.pop();
}

Sequence.prototype._get = function(i) {
    return this.sequence[i];
}

Sequence.prototype.count = function() {
    return this.sequence.length;
}

Sequence.prototype.skip = function(skip) {
    var result = new Sequence();
    for(var i=skip; i<this.sequence.length; i++) {
        // TODO Should we also deep copy this.selection[i]
        result.push(this.sequence[i]);
    }
    return result;
}

Sequence.prototype.limit = function(limit) {
    var result = new Sequence();
    for(var i=0; i<Math.min(limit,this.sequence.length); i++) {
        // TODO Should we also deep copy this.selection[i]
        result.push(this.sequence[i]);
    }
    return result;
}

Sequence.prototype.pluck = function(keys) {
    var result = new Sequence();
    for(var i=0; i<this.sequence.length; i++) {
        result.push(helper.pluck(this.sequence[i], keys));
    }
    return result;
}
Sequence.prototype.without = function(keys) {
    var result = new Sequence();
    for(var i=0; i<this.sequence.length; i++) {
        result.push(helper.without(this.sequence[i], keys));
    }
    return result;
}


Sequence.prototype.slice = function(start, end, options) {
    var result = new Sequence();
    console.log(options);

    var leftBound = options.left_bound || "closed";
    var rightBound = options.right_bound || "open";

    // TODO Check arguments
    if ((typeof start === 'number') && (start < 0)) {
        start = this.sequence.length+start;
    }

    if (end === undefined) {
        end = this.sequence.length;
    }
    else if ((typeof end === 'number') && (end < 0)) {
        end = this.sequence.length+end;
    }

    if (leftBound === "open") {
        start++;
    }
    if (rightBound === "closed") {
        end++;
    }

    for(var i=start; i<end; i++) {
        if (i >=this.sequence.length) { break }
        // TODO Should we also deep copy this.selection[i]
        result.push(this.sequence[i]);
    }
    return result;
}

Sequence.prototype.nth = function(index, query) {
    if (index < 0) {
        index = this.sequence.length+index;
    }
    if (index >= this.sequence.length) {
        throw new Error.ReqlRuntimeError("Index out of bounds: "+index, query.frames)
    }
    return this.sequence[index]
}

Sequence.prototype.indexesOf = function(predicate, query) {
    var result = new Sequence();

    var value, varId, predicateResult;
    if ((Array.isArray(predicate) === false) || (predicate[0] !== 69)) {
        value = query.evaluate(predicate, query)
    }

    for(var i=0; i<this.sequence.length; i++) {
        if (value !== undefined) {
            if (helper.eq(this.sequence[i], value)) {
                result.push(i);
            }
        }
        else { // We have a function here
            console.log(predicate);
            varId = predicate[1][0][1][0];
            query.context[varId] = this.sequence[i];

            predicateResult = query.evaluate(predicate, query);
            if ((predicateResult !== null) && (predicateResult !== false)) {
                result.push(i);
            }
            delete query.context[varId];
        }
    }
    return result;
}

Sequence.prototype.isEmpty = function() {
    return this.sequence.length === 0;
}


Sequence.prototype.map = function(fn, query) {
    //TODO Check that fn is a function
    var result = new Sequence();
    for(var i=0; i<this.sequence.length; i++) {
        if (fn[0] === 69) { // newValue is a FUNC term
            var varId = fn[1][0][1]; // 0 to select the array, 1 to select the first element
            query.context[varId] = this.sequence[i];
        }
        result.push(query.evaluate(fn))
        if (fn[0] === 69) {
            var varId = fn[1][0][1];
            delete query.context[varId]
        }
    }
    return result;
}

Sequence.prototype.concatMap = function(fn, query) {
    //TODO Check that fn is a function
    var result = new Sequence();
    var partial;
    for(var i=0; i<this.sequence.length; i++) {
        if (fn[0] === 69) { // newValue is a FUNC term
            var varId = fn[1][0][1]; // 0 to select the array, 1 to select the first element
            query.context[varId] = this.sequence[i];
        }
        partial = query.evaluate(fn);
        //TODO Check partial type
        for(var k=0; k<partial.length; k++) {
            result.push(partial._get(k));
        }
        if (fn[0] === 69) {
            var varId = fn[1][0][1];
            delete query.context[varId]
        }
    }
    return result;
}
Sequence.prototype.withFields = function(fields) {
    // fields is a Sequence

    var result = new Sequence();
    var valid, element;
    for(var i=0; i<this.sequence.length; i++) {
        valid = true;
        for(var j=0; j<fields.length; j++) {
            if (this._get(i)[fields._get(j)] === undefined) {
                valid = false;
                break;
            }
        }
        if (valid === true) {
            element = {};
            for(var j=0; j<fields.length; j++) {
                element[fields._get(j)] = this._get(i)[fields._get(j)]
            }

            result.push(element)
        }
    }
    return result;
}

Sequence.prototype.orderBy = function(fields, options, query) {
    var result = new Sequence(this.sequence);
    result.sequence.sort(function(left, right) {
        var index = 0;
        var field, leftValue, rightValue;

        if (typeof options.index === "string") {
            //TODO Send the appropriate message
            throw new Error("Cannot use an index on a sequence")
        }

        while(index <= fields.length) {
            field = fields[index];
            if (Array.isArray(field) && (field[0] === 69)) { // newValue is a FUNC term
                var varId = field[1][0][1]; // 0 to select the array, 1 to select the first element
                query.context[varId] = left;
                leftValue = query.evaluate(field);
                delete query.context[varId];

                query.context[varId] = right;
                rightValue = query.evaluate(field);
                delete query.context[varId];
            }
            else {
                field = query.evaluate(field);

                //TODO Are we really doing that? Seriously?
                leftValue = (typeof left.getField === "function") ? left.getfield(field) : left[field];
                rightValue = (typeof right.getField === "function") ? right.getfield(field) : right[field];
            }

            if (helper.gt(leftValue, rightValue)) {
                return 1
            }
            else if (helper.eq(leftValue, rightValue)) {
                index++;
            }
            else {
                return -1
            }
        }
        return 0;
    });
    return result;
}


Sequence.prototype.toDatum = function() {
    var result = [];
    for(var i=0; i<this.sequence.length; i++) {
        if (typeof this.sequence[i].toDatum === "function") {
            result.push(this.sequence[i].toDatum());
        }
        else {
            result.push(helper.toDatum(this.sequence[i]));
        }
    }
    return result;
}


module.exports = Sequence
