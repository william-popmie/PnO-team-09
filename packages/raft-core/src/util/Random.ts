export interface Random {
    nextInt(min: number, max: number): number;
    nextFloat(): number;
}

export class SeededRandom implements Random {
    private seed: number;

    constructor(seed: number) {
        this.seed = seed;
    }

    nextFloat(): number {

        const a = 1664525; // LCG
        const c = 1013904223;
        const m = 2 ** 32;

        this.seed = (a * this.seed + c) % m;
        return this.seed / m;
    }

    nextInt(min: number, max: number): number {
        if (min >= max) {
            throw new Error("min must be less than max");
        }

        const range = max - min + 1;
        return min + Math.floor(this.nextFloat() * range);
    }

    reset(seed: number) {
        this.seed = seed;
    }

    getSeed(): number {
        return this.seed;
    }
}

export class SystemRandom implements Random {
    nextInt(min: number, max: number): number {
        if (min >= max) {
            throw new Error("min must be less than max");
        }
        const range = max - min + 1;
        return min + Math.floor(Math.random() * range);
    }

    nextFloat(): number {
        return Math.random();
    }
}