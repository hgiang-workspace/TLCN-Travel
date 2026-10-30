export type LoadState='ready'|'loading'|'empty'|'error'|'insufficient'
export type DomainId='all'|'destination'|'stay'|'food'|'transport'
export interface Kpi{label:string;value:string;note?:string}
export interface Aspect{label:string;score:number}
export interface Place{id:string;name:string;domain:Exclude<DomainId,'all'>;rating?:number;reviewCount?:number;match:string;location:string;aspects:Aspect[]}
export interface GeoContext{provinceId:string}
