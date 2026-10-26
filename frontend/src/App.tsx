import { Navigate, Route, Routes } from 'react-router'
import { AppShell } from './shared/layout/AppShell'
import { OverviewPage } from './features/overview/OverviewPage'
import { MarketPage } from './features/market/MarketPage'
import { ExplorePage } from './features/explore/ExplorePage'
import { PlacePage } from './features/place/PlacePage'
export default function App(){return <Routes><Route element={<AppShell/>}><Route index element={<OverviewPage/>}/><Route path="market" element={<MarketPage/>}/><Route path="explore" element={<ExplorePage/>}/><Route path="places" element={<PlacePage/>}/><Route path="*" element={<Navigate to="/" replace/>}/></Route></Routes>}
