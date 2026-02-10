export class RaftError extends Error {
    constructor(
        message: string,
        public readonly code: string
    ) {
        super(message);
        this.name = 'RaftError';
        Object.setPrototypeOf(this, RaftError.prototype); // anders kapotte portotype chain 
    }
}